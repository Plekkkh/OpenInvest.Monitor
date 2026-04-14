import logging
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional
from django.utils.timezone import is_aware, make_aware, now
from django.core.cache import cache

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
        if not quotation:
            return Decimal('0')
        return quotation_to_decimal(quotation)

    def _map_operation(self, op_type: OperationType, payment: Decimal) -> Optional[str]:
        """Маппинг типов операций Т-Инвестиций в локальные типы"""
        mapping = {
            # Сделки
            OperationType.OPERATION_TYPE_BUY: 'buy',
            OperationType.OPERATION_TYPE_BUY_CARD: 'buy',
            OperationType.OPERATION_TYPE_BUY_MARGIN: 'buy',
            OperationType.OPERATION_TYPE_DELIVERY_BUY: 'buy',
            OperationType.OPERATION_TYPE_SELL: 'sell',
            OperationType.OPERATION_TYPE_SELL_CARD: 'sell',
            OperationType.OPERATION_TYPE_SELL_MARGIN: 'sell',
            OperationType.OPERATION_TYPE_DELIVERY_SELL: 'sell',
            OperationType.OPERATION_TYPE_BOND_REPAYMENT_FULL: 'repayment',

            # Начисления
            OperationType.OPERATION_TYPE_DIVIDEND: 'dividend',
            OperationType.OPERATION_TYPE_DIV_EXT: 'dividend',
            OperationType.OPERATION_TYPE_DIVIDEND_TRANSFER: 'dividend',
            OperationType.OPERATION_TYPE_COUPON: 'coupon',
            OperationType.OPERATION_TYPE_BOND_REPAYMENT: 'amortization',

            # Расходы
            OperationType.OPERATION_TYPE_BROKER_FEE: 'commission',
            OperationType.OPERATION_TYPE_SERVICE_FEE: 'commission',
            OperationType.OPERATION_TYPE_MARGIN_FEE: 'commission',
            OperationType.OPERATION_TYPE_SUCCESS_FEE: 'commission',
            OperationType.OPERATION_TYPE_TRACK_MFEE: 'commission',
            OperationType.OPERATION_TYPE_TRACK_PFEE: 'commission',
            OperationType.OPERATION_TYPE_CASH_FEE: 'commission',
            OperationType.OPERATION_TYPE_OUT_FEE: 'commission',
            OperationType.OPERATION_TYPE_OUT_STAMP_DUTY: 'commission',
            OperationType.OPERATION_TYPE_OUTPUT_PENALTY: 'commission',
            OperationType.OPERATION_TYPE_ADVICE_FEE: 'commission',
            OperationType.OPERATION_TYPE_OTHER_FEE: 'commission',
            OperationType.OPERATION_TYPE_OVER_COM: 'commission',

            OperationType.OPERATION_TYPE_TAX: 'tax',
            OperationType.OPERATION_TYPE_BOND_TAX: 'tax',
            OperationType.OPERATION_TYPE_DIVIDEND_TAX: 'tax',
            OperationType.OPERATION_TYPE_BENEFIT_TAX: 'tax',
            OperationType.OPERATION_TYPE_TAX_CORRECTION: 'tax',
            OperationType.OPERATION_TYPE_TAX_PROGRESSIVE: 'tax',
            OperationType.OPERATION_TYPE_BOND_TAX_PROGRESSIVE: 'tax',
            OperationType.OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE: 'tax',
            OperationType.OPERATION_TYPE_BENEFIT_TAX_PROGRESSIVE: 'tax',
            OperationType.OPERATION_TYPE_TAX_CORRECTION_PROGRESSIVE: 'tax',
            OperationType.OPERATION_TYPE_TAX_REPO_PROGRESSIVE: 'tax',
            OperationType.OPERATION_TYPE_TAX_REPO: 'tax',
            OperationType.OPERATION_TYPE_TAX_REPO_HOLD: 'tax',
            OperationType.OPERATION_TYPE_TAX_REPO_HOLD_PROGRESSIVE: 'tax',
            OperationType.OPERATION_TYPE_TAX_CORRECTION_COUPON: 'tax',

            OperationType.OPERATION_TYPE_TAX_REPO_REFUND: 'tax_refund',
            OperationType.OPERATION_TYPE_TAX_REPO_REFUND_PROGRESSIVE: 'tax_refund',

            # Валюта
            OperationType.OPERATION_TYPE_INPUT: 'deposit',
            OperationType.OPERATION_TYPE_INP_MULTI: 'deposit',
            OperationType.OPERATION_TYPE_INPUT_SWIFT: 'deposit',
            OperationType.OPERATION_TYPE_INPUT_ACQUIRING: 'deposit',

            OperationType.OPERATION_TYPE_OUTPUT: 'withdrawal',
            OperationType.OPERATION_TYPE_OUT_MULTI: 'withdrawal',
            OperationType.OPERATION_TYPE_OUTPUT_SWIFT: 'withdrawal',
            OperationType.OPERATION_TYPE_OUTPUT_ACQUIRING: 'withdrawal',

            OperationType.OPERATION_TYPE_OVERNIGHT: 'other_income',
            OperationType.OPERATION_TYPE_OVER_INCOME: 'other_income',
            OperationType.OPERATION_TYPE_ACCRUING_VARMARGIN: 'other_income',
            OperationType.OPERATION_TYPE_WRITING_OFF_VARMARGIN: 'other_expense',
        }
        res = mapping.get(op_type)
        if res:
            return res

        if payment > 0:
            return 'other_income'
        elif payment < 0:
            return 'other_expense'

        return None

    def _map_instrument_type(self, instrument_type: str) -> str:
        mapping = {
            'share': 'Share',
            'bond': 'Bond',
            'etf': 'ETF',
            'currency': 'Currency',
        }
        return mapping.get(instrument_type, 'Share')

    def _build_instruments_index(self, instruments_cache: InstrumentsCache) -> dict:
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

    def _resolve_asset(self, instrument_index: dict, figi: str, instrument_uid: str) -> Optional[Asset]:
        """Получение или создание актива по figi/uid из плоского индекса"""
        if not figi and not instrument_uid:
            return None

        try:
            instrument = instrument_index.get(instrument_uid) or instrument_index.get(figi)

            if instrument:
                asset, created = Asset.objects.get_or_create(
                    ticker=instrument['ticker'],
                    defaults={
                        'isin': instrument['isin'],
                        'name': instrument['name'],
                        'asset_type': self._map_instrument_type(instrument['instrument_type']),
                        'currency': instrument['currency']
                    }
                )
                return asset
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

        broker_account = valid_accounts[0]
        self.account.provider_account_id = broker_account.id
        self.account.save(update_fields=['provider_account_id'])
        return broker_account.id

    def _get_account_opened_date(self, client, account_id: str) -> Optional[datetime]:
        """Возвращает дату открытия счета для оптимизации синхронизации"""
        try:
            accounts_resp = client.users.get_accounts()
            for acc in accounts_resp.accounts:
                if acc.id == account_id:
                    return acc.opened_date
        except Exception as e:
            logger.warning("Не удалось получить дату открытия счета %s: %s", account_id, e)
        return None

    def sync_operations(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> int:
        """Скачивает и сохраняет операции"""
        try:
            with RetryingClient(self.token, settings=self.retry_settings) as client:
                account_id = self._get_account_id(client)

                # Пытаемся получить кеш инструментов
                CACHE_KEY = 't_invest_instruments_index'
                instrument_index = cache.get(CACHE_KEY)

                if not instrument_index:
                    settings = InstrumentsCacheSettings()
                    instruments_cache = InstrumentsCache(
                        settings=settings,
                        instruments_service=client.instruments
                    )

                    # Инициализация плоского словаря для мгновенного поиска активов без class_code
                    instrument_index = self._build_instruments_index(instruments_cache)

                    # Сохраняем в кэш на 24 часа (86400 секунд)
                    cache.set(CACHE_KEY, instrument_index, timeout=86400)

                if from_date is None:
                    # Оптимизация: используем дату открытия счета вместо 2000-01-01
                    opened_date = self._get_account_opened_date(client, account_id)
                    if opened_date:
                        from_date = opened_date
                    else:
                        from_date = datetime(2000, 1, 1, tzinfo=timezone.utc)

                if to_date is None:
                    to_date = now()

                # Приводим даты к UTC aware формату
                if not is_aware(from_date):
                    from_date = make_aware(from_date)
                if not is_aware(to_date):
                    to_date = make_aware(to_date)

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
                        # Пропускаем неподдерживаемые типы (отмены и т.д.)
                        continue

                    # Учитываем, что комиссии приходят как независимые операции с указанием parent_operation_id
                    parent_op_id = getattr(op, 'parent_operation_id', None)
                    if parent_op_id:
                        parent_links[op.id] = parent_op_id

                    # Проверяем на дубликаты
                    if op.id in existing_ids:
                        continue

                    asset = self._resolve_asset(instrument_index, op.figi, op.instrument_uid)

                    price = self._quotation_to_decimal(op.price)
                    qty = op.quantity if hasattr(op, 'quantity') else 0
                    if qty == 0:
                        # Если qty = 0, значит это не сделка (налог, комиссия, ввод)
                        # Записываем сумму как price
                        price = abs(payment)
                        qty = 1

                    yield_amount = self._quotation_to_decimal(getattr(op, 'yield_', None))
                    commission_amount = self._quotation_to_decimal(getattr(op, 'commission', None))
                    accrued_int = self._quotation_to_decimal(getattr(op, 'accrued_int', None))

                    new_transactions.append(Transaction(
                        account=self.account,
                        external_id=op.id,
                        asset=asset,
                        operation_type=op_type,
                        quantity=Decimal(str(qty)),
                        price_per_unit=price,
                        date=op.date,
                        yield_amount=yield_amount,
                        commission_amount=commission_amount,
                        accrued_int=accrued_int
                    ))

                saved_count = 0
                if new_transactions:
                    Transaction.objects.bulk_create(new_transactions, ignore_conflicts=True)
                    saved_count = len(new_transactions)

                # Восстанавливаем связи дочерних операций (комиссий) с родительскими
                if parent_links:
                    # Получаем ID операций в базе
                    all_related_external_ids = list(parent_links.keys()) + list(parent_links.values())
                    db_txs = Transaction.objects.filter(
                        account=self.account,
                        external_id__in=all_related_external_ids
                    ).values('id', 'external_id')

                    # Карта external_id -> django_id
                    ext_to_id = {tx['external_id']: tx['id'] for tx in db_txs}

                    # Обновляем связи
                    to_update = []
                    for child_ext_id, parent_ext_id in parent_links.items():
                        child_db_id = ext_to_id.get(child_ext_id)
                        parent_db_id = ext_to_id.get(parent_ext_id)
                        if child_db_id and parent_db_id:
                            to_update.append(Transaction(id=child_db_id, parent_transaction_id=parent_db_id))

                    if to_update:
                        Transaction.objects.bulk_update(to_update, ['parent_transaction_id'])

                return saved_count

        except Exception as e:
            logger.error("Ошибка при синхронизации операций: %s", str(e))
            raise ValueError(f"Ошибка при синхронизации операций: {str(e)}")

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

                positions = []
                currencies = []

                # В Tinkoff API активы и валюты могут лежать в response.positions
                for pos in response.positions:
                    # Извлекаем тип инструмента в виде строки (напр. 'share', 'currency')
                    instrument_type = str(pos.instrument_type).split('.')[-1].lower()

                    if instrument_type == 'currency' or pos.instrument_type == 'currency':
                        # Для валют количество (quantity) - это их баланс
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
                            'ticker': getattr(pos, 'ticker', None) or getattr(pos, 'instrument_uid', pos.figi),
                            'instrument_type': instrument_type,
                            'quantity': float(self._quotation_to_decimal(pos.quantity)),
                            'average_buy_price': float(self._quotation_to_decimal(pos.average_position_price)),
                            'current_price': float(self._quotation_to_decimal(pos.current_price)),
                            'expected_yield': float(self._quotation_to_decimal(pos.expected_yield)),
                            'current_nkd': float(self._quotation_to_decimal(pos.current_nkd)),
                        })

                total_amount = float(
                    self._quotation_to_decimal(response.total_amount_portfolio)
                )

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
            raise ValueError(f"Ошибка при получении портфеля: {str(e)}")
