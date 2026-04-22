import logging
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any
from django.utils.timezone import is_aware, make_aware, now
from django.core.cache import cache

from portfolio.services.t_invest_constants import OPERATION_MAPPING, INSTRUMENT_TYPE_MAPPING

from t_tech.invest import (
    AccessLevel,
    OperationState,
    OperationType,
    GetOperationsByCursorRequest
)
from t_tech.invest.schemas import Quotation, MoneyValue
from t_tech.invest.retrying.sync.client import RetryingClient
from t_tech.invest.retrying.settings import RetryClientSettings
from t_tech.invest.caching.instruments_cache.instruments_cache import InstrumentsCache
from t_tech.invest.caching.instruments_cache.settings import InstrumentsCacheSettings
from t_tech.invest.utils import quotation_to_decimal

from portfolio.models import BrokerAccount, Transaction, Asset


logger = logging.getLogger(__name__)


class TInvestServiceError(RuntimeError):
    """Ошибка уровня сервиса при работе с API Т-Инвестиций."""


class TInvestService:
    """Сервис для интеграции с API Т-Инвестиций"""

    def __init__(self, account: BrokerAccount) -> None:
        self.account = account
        self.token = account.api_token
        if not self.token:
            raise ValueError(f"Для счета {account.name} не установлен API-токен.")

        self.retry_settings = RetryClientSettings(
            use_retry=True,
            max_retry_attempt=3
        )

    def _quotation_to_decimal(self, quotation: Quotation | MoneyValue) -> Decimal:
        """Вспомогательный метод для конвертации Quotation/MoneyValue в Decimal"""
        return quotation_to_decimal(quotation) if quotation else Decimal('0')

    def _map_operation(self, op_type: OperationType, payment: Decimal) -> str | None:
        """Маппинг типов операций Т-Инвестиций в локальные типы"""
        if op_type in OPERATION_MAPPING:
            return OPERATION_MAPPING[op_type]

        if payment > 0:
            return 'other_income'
        if payment < 0:
            return 'other_expense'
        return None

    def _map_instrument_type(self, instrument_type: str) -> str:
        return INSTRUMENT_TYPE_MAPPING.get(instrument_type, 'Share')

    @staticmethod
    def _apply_asset_defaults(asset: Asset, defaults: dict[str, Any]) -> Asset:
        """Обновляет поля актива только измененными значениями."""
        updated_fields: list[str] = []
        for key, value in defaults.items():
            if value and getattr(asset, key) != value:
                setattr(asset, key, value)
                updated_fields.append(key)
        if updated_fields:
            asset.save(update_fields=updated_fields)
        return asset

    def _build_instruments_index(self, instruments_cache: InstrumentsCache) -> dict[str, dict[str, Any]]:
        """
        Строит локальный O(1) индекс по всем инструментам.

        Обоснование: стандартные методы SDK `share_by`, `bond_by` из InstrumentsCache
        требуют передачи `class_code` (внутренний ключ кэша: (class_code, id)),
        которого у нас нет в ответе get_operations_by_cursor.
        Поэтому мы единоразово собираем плоский словарь по UID и FIGI.
        """
        index = {}
        for group_name in ('shares', 'bonds', 'etfs', 'currencies'):
            try:
                method = getattr(instruments_cache, group_name)
                collection = method()
                for inst in collection.instruments:
                    inst_type = getattr(inst, 'instrument_type', type(inst).__name__).lower()
                    # Сохраняем только необходимые поля в виде словаря для совместимости с Redis/Memcached
                    # и предотвращения ошибок сериализации
                    data = {
                        'instrument_uid': getattr(inst, 'uid', None),
                        'figi': getattr(inst, 'figi', None),
                        'ticker': inst.ticker,
                        'isin': getattr(inst, 'isin', ''),
                        'name': inst.name,
                        'instrument_type': inst_type,
                        'currency': getattr(inst, 'currency', 'rub').upper()
                    }
                    if getattr(inst, 'uid', None):
                        index[inst.uid] = data
                    if getattr(inst, 'figi', None):
                        index[inst.figi] = data
            except Exception as e:
                logger.warning("Ошибка при загрузке индекса инструментов (%s): %s", group_name, e)
        return index

    def _resolve_asset(
        self,
        instrument_index: dict[str, dict[str, Any]],
        figi: str,
        instrument_uid: str
    ) -> Asset | None:
        """Получение или создание актива по figi/uid из плоского индекса"""
        if not figi and not instrument_uid:
            return None

        try:
            instrument = instrument_index.get(instrument_uid) or instrument_index.get(figi)
            asset: Asset | None = None

            if instrument_uid:
                asset = Asset.objects.filter(instrument_uid=instrument_uid).first()
            if not asset and figi:
                asset = Asset.objects.filter(figi=figi).first()

            if instrument:
                defaults = {
                    'instrument_uid': instrument.get('instrument_uid') or instrument_uid,
                    'figi': instrument.get('figi') or figi,
                    'ticker': instrument['ticker'],
                    'isin': instrument['isin'],
                    'name': instrument['name'],
                    'asset_type': self._map_instrument_type(instrument['instrument_type']),
                    'currency': instrument['currency']
                }

                if asset:
                    return self._apply_asset_defaults(asset, defaults)

                asset = Asset.objects.filter(ticker=defaults['ticker']).first()
                if asset:
                    return self._apply_asset_defaults(asset, defaults)

                return Asset.objects.create(
                    instrument_uid=defaults['instrument_uid'],
                    figi=defaults['figi'],
                    ticker=defaults['ticker'],
                    isin=defaults['isin'],
                    name=defaults['name'],
                    asset_type=defaults['asset_type'],
                    currency=defaults['currency']
                )
        except Exception as e:
            logger.warning(
                "Ошибка при получении инструмента (figi=%s, uid=%s): %s",
                figi, instrument_uid, str(e)
            )

        return None

    def _get_account_id(self, client) -> str:
        """Получает ID счета для синхронизации"""
        if self.account.provider_account_id:
            return self.account.provider_account_id

        accounts_resp = client.users.get_accounts()

        valid_levels = (
            AccessLevel.ACCOUNT_ACCESS_LEVEL_FULL_ACCESS,
            AccessLevel.ACCOUNT_ACCESS_LEVEL_READ_ONLY
        )
        valid_accounts = [
            acc for acc in accounts_resp.accounts if acc.access_level in valid_levels
        ]

        if not valid_accounts:
            raise ValueError("Брокерские счета с доступными правами не найдены.")

        if len(valid_accounts) > 1:
            raise ValueError(
                "Найдено несколько счетов у брокера. Укажите provider_account_id для нужного счета."
            )

        broker_account = valid_accounts[0]
        self.account.provider_account_id = broker_account.id
        self.account.save(update_fields=['provider_account_id'])
        return broker_account.id

    def _get_account_opened_date(self, client, account_id: str) -> datetime | None:
        """Возвращает дату открытия счета для оптимизации синхронизации"""
        try:
            accounts_resp = client.users.get_accounts()
            for acc in accounts_resp.accounts:
                if acc.id == account_id:
                    return acc.opened_date
        except Exception as e:
            logger.warning("Не удалось получить дату открытия счета %s: %s", account_id, e)
        return None

    def _get_or_build_instruments_index(self, client) -> dict[str, dict[str, Any]]:
        CACHE_KEY = 't_invest_instruments_index'
        instrument_index = cache.get(CACHE_KEY)

        if not instrument_index:
            settings = InstrumentsCacheSettings()
            instruments_cache = InstrumentsCache(
                settings=settings,
                instruments_service=client.instruments
            )
            instrument_index = self._build_instruments_index(instruments_cache)
            cache.set(CACHE_KEY, instrument_index, timeout=86400)
        return instrument_index

    def _fetch_operations_from_api(self, client, account_id: str, from_date: datetime, to_date: datetime) -> list:
        request = GetOperationsByCursorRequest(
            account_id=account_id,
            from_=from_date,
            to=to_date,
            state=OperationState.OPERATION_STATE_EXECUTED,
            limit=1000
        )
        operations_response = client.operations.get_operations_by_cursor(request)
        operations_items = operations_response.items

        while operations_response.has_next:
            request.cursor = operations_response.next_cursor
            operations_response = client.operations.get_operations_by_cursor(request)
            operations_items.extend(operations_response.items)

        return operations_items

    def _process_and_save_operations(
        self,
        operations_items,
        instrument_index: dict[str, dict[str, Any]],
        from_date: datetime,
        to_date: datetime
    ) -> int:
        existing_ids = set(Transaction.objects.filter(
            account=self.account,
            date__range=(from_date, to_date)
        ).values_list('external_id', flat=True))

        new_transactions = []
        parent_links = {}

        for op in operations_items:
            payment = self._quotation_to_decimal(op.payment)
            op_type = self._map_operation(op.type, payment)
            if not op_type:
                continue

            parent_op_id = getattr(op, 'parent_operation_id', None)
            if parent_op_id:
                parent_links[op.id] = parent_op_id

            if op.id in existing_ids:
                continue

            asset = self._resolve_asset(instrument_index, op.figi, op.instrument_uid)
            price = self._quotation_to_decimal(op.price)
            qty = op.quantity if hasattr(op, 'quantity') else 0
            if qty == 0:
                price = abs(payment)
                qty = 1

            new_transactions.append(Transaction(
                account=self.account,
                external_id=op.id,
                asset=asset,
                operation_type=op_type,
                quantity=Decimal(str(qty)),
                price_per_unit=price,
                date=op.date,
                yield_amount=self._quotation_to_decimal(getattr(op, 'yield_', None)),
                commission_amount=self._quotation_to_decimal(getattr(op, 'commission', None)),
                accrued_int=self._quotation_to_decimal(getattr(op, 'accrued_int', None))
            ))

        saved_count = 0
        if new_transactions:
            Transaction.objects.bulk_create(new_transactions, ignore_conflicts=True)
            saved_count = len(new_transactions)

        self._restore_parent_links(parent_links)
        return saved_count

    def _restore_parent_links(self, parent_links: dict):
        if not parent_links:
            return

        all_related_external_ids = list(parent_links.keys()) + list(parent_links.values())
        db_txs = Transaction.objects.filter(
            account=self.account,
            external_id__in=all_related_external_ids
        ).values('id', 'external_id')

        ext_to_id = {tx['external_id']: tx['id'] for tx in db_txs}

        to_update = []
        for child_ext_id, parent_ext_id in parent_links.items():
            child_db_id = ext_to_id.get(child_ext_id)
            parent_db_id = ext_to_id.get(parent_ext_id)
            if child_db_id and parent_db_id:
                to_update.append(Transaction(id=child_db_id, parent_transaction_id=parent_db_id))

        if to_update:
            Transaction.objects.bulk_update(to_update, ['parent_transaction_id'])

    def sync_operations(
        self,
        from_date: datetime | None = None,
        to_date: datetime | None = None
    ) -> int:
        """Скачивает и сохраняет операции"""
        try:
            with RetryingClient(self.token, settings=self.retry_settings) as client:
                account_id = self._get_account_id(client)
                instrument_index = self._get_or_build_instruments_index(client)

                if from_date is None:
                    opened_date = self._get_account_opened_date(client, account_id)
                    from_date: datetime = opened_date if opened_date else datetime(2000, 1, 1, tzinfo=timezone.utc)

                if to_date is None:
                    to_date: datetime = now()

                if not is_aware(from_date):
                    from_date: datetime = make_aware(from_date)
                if not is_aware(to_date):
                    to_date: datetime = make_aware(to_date)

                operations_items = self._fetch_operations_from_api(client, account_id, from_date, to_date)
                saved_count = self._process_and_save_operations(
                    operations_items, instrument_index, from_date, to_date
                )
                return saved_count

        except Exception as e:
            logger.error("Ошибка при синхронизации операций: %s", str(e))
            raise TInvestServiceError(f"Ошибка при синхронизации операций: {str(e)}") from e

    def _parse_positions_and_currencies(self, client_response_positions) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Разбирает ответ API на позиции и валюты"""
        positions = []
        currencies = []

        for pos in client_response_positions:
            instrument_type = str(pos.instrument_type).split('.')[-1].lower()

            if instrument_type == 'currency' or pos.instrument_type == 'currency':
                currency_name = 'rub'
                has_avg_price = hasattr(pos, 'average_position_price')
                has_currency_in_avg = (has_avg_price and hasattr(pos.average_position_price, 'currency'))

                if has_currency_in_avg:
                    currency_name = getattr(pos.average_position_price, 'currency')
                elif hasattr(pos, 'currency'):
                    currency_name = getattr(pos, 'currency')

                currencies.append({
                    'currency': str(currency_name).lower(),
                    'balance': float(self._quotation_to_decimal(pos.quantity))
                })
            else:
                positions.append({
                    'figi': pos.figi,
                    'instrument_uid': getattr(pos, 'instrument_uid', ''),
                    'ticker': getattr(pos, 'ticker', None) or getattr(pos, 'instrument_uid', pos.figi),
                    'instrument_type': instrument_type,
                    'quantity': float(self._quotation_to_decimal(pos.quantity)),
                    'average_buy_price': float(self._quotation_to_decimal(pos.average_position_price)),
                    'current_price': float(self._quotation_to_decimal(pos.current_price)),
                    'expected_yield': float(self._quotation_to_decimal(pos.expected_yield)),
                    'current_nkd': float(self._quotation_to_decimal(pos.current_nkd)),
                })
        return positions, currencies

    def get_portfolio(self) -> dict:
        """Получает текущее состояние портфеля из API"""
        try:
            with RetryingClient(self.token, settings=self.retry_settings) as client:
                account_id = self._get_account_id(client)

                # Кэширование портфеля на 60 секунд
                CACHE_KEY = f't_invest_portfolio_{account_id}'
                cached_portfolio = cache.get(CACHE_KEY)
                if cached_portfolio:
                    return cached_portfolio

                response = client.operations.get_portfolio(account_id=account_id)
                positions, currencies = self._parse_positions_and_currencies(response.positions)

                total_amount = float(self._quotation_to_decimal(response.total_amount_portfolio))

                result = {
                    'account_id': account_id,
                    'total_amount': total_amount,
                    'positions': positions,
                    'currencies': currencies,
                    'updated_at': now()
                }

                # Сохраняем в кэш на 60 секунд
                cache.set(CACHE_KEY, result, timeout=60)

                return result
        except Exception as e:
            logger.error("Ошибка при получении портфеля: %s", str(e))
            raise TInvestServiceError(f"Ошибка при получении портфеля: {str(e)}") from e
