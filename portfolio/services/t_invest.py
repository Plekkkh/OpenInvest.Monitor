import logging
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional
from django.utils.timezone import is_aware, make_aware, now
from django.core.cache import cache

from t_tech.invest import AccessLevel, OperationState, OperationType, GetOperationsByCursorRequest
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
        
        self.retry_settings = RetryClientSettings(use_retry=True, max_retry_attempt=3)

    def _quotation_to_decimal(self, quotation: Quotation | MoneyValue) -> Decimal:
        """Вспомогательный метод для конвертации Quotation/MoneyValue в Decimal"""
        if not quotation:
            return Decimal('0')
        return quotation_to_decimal(quotation)

    def _map_operation(self, op_type: OperationType) -> Optional[str]:
        """Маппинг типов операций Т-Инвестиций в локальные типы"""
        mapping = {
            OperationType.OPERATION_TYPE_BUY: 'Buy',
            OperationType.OPERATION_TYPE_SELL: 'Sell',
            OperationType.OPERATION_TYPE_DIVIDEND: 'Dividend',
            OperationType.OPERATION_TYPE_TAX: 'Tax',
            OperationType.OPERATION_TYPE_DIVIDEND_TAX: 'Tax',
            OperationType.OPERATION_TYPE_BROKER_FEE: 'Fee',
            OperationType.OPERATION_TYPE_MARGIN_FEE: 'Fee',
            OperationType.OPERATION_TYPE_INPUT: 'Deposit',
            OperationType.OPERATION_TYPE_OUTPUT: 'Withdrawal',
        }
        return mapping.get(op_type)

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
            logger.warning("Ошибка при получении инструмента (figi=%s, uid=%s): %s", figi, instrument_uid, str(e))

        return None

    def _get_account_id(self, client) -> str:
        """Получает ID счета для синхронизации"""
        if self.account.provider_account_id:
            return self.account.provider_account_id
            
        accounts_resp = client.users.get_accounts()

        valid_accounts = [
            acc for acc in accounts_resp.accounts 
            if acc.access_level in (AccessLevel.ACCOUNT_ACCESS_LEVEL_FULL_ACCESS, AccessLevel.ACCOUNT_ACCESS_LEVEL_READ_ONLY)
        ]

        if not valid_accounts:
            raise ValueError("Брокерские счета с доступными правами (FULL_ACCESS или READ_ONLY) не найдены в API.")
            
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

    def sync_operations(self, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None) -> int:
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
                        settings=settings, instruments_service=client.instruments
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

                # Массовое добавление для ускорения
                new_transactions = []
                existing_ids = set(Transaction.objects.filter(
                    account=self.account,
                    date__range=(from_date, to_date)
                ).values_list('external_id', flat=True))

                for op in operations_items:
                    op_type = self._map_operation(op.type)
                    if not op_type:
                        continue # Пропускаем неподдерживаемые типы (например, отмены, блокировки)

                    # Проверяем на дубликаты
                    if op.id in existing_ids:
                        continue

                    asset = self._resolve_asset(instrument_index, op.figi, op.instrument_uid)

                    price = self._quotation_to_decimal(op.price)
                    qty = op.quantity if hasattr(op, 'quantity') else 0
                    if qty == 0:
                        # Если qty = 0, значит это не сделка (налог, комиссия, ввод)
                        # Записываем сумму как price
                        price = abs(self._quotation_to_decimal(op.payment))
                        qty = 1

                    new_transactions.append(Transaction(
                        account=self.account,
                        external_id=op.id,
                        asset=asset,
                        operation_type=op_type,
                        quantity=Decimal(str(qty)),
                        price_per_unit=price,
                        date=op.date
                    ))

                if new_transactions:
                    # ignore_conflicts=True используется для избежания ошибок уникальности (например external_id)
                    Transaction.objects.bulk_create(new_transactions, ignore_conflicts=True)

                return len(new_transactions)

        except Exception as e:
            logger.error("Ошибка при синхронизации операций: %s", str(e))
            raise ValueError(f"Ошибка при синхронизации операций: {str(e)}")
