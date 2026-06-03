import pandas as pd
import pyxirr
import logging
from decimal import Decimal
from typing import Dict, Any, Optional, TypedDict
from datetime import datetime

from django.utils.timezone import now
from django.db.models import Sum, F, Q, QuerySet
from portfolio.models import BrokerAccount, Transaction
from portfolio.services.price_provider import PriceProvider, TransactionPriceProvider
from portfolio.services.t_invest import TInvestService, TInvestServiceError


logger = logging.getLogger(__name__)


class AllocationGroup(TypedDict):
    name: str
    current_value: Decimal
    invested: Decimal
    yield_amount: Decimal
    share: Decimal
    yield_percent: Decimal


class AnalyticsService:
    _main_operation_types = {
        'buy',
        'sell',
        'deposit',
        'withdrawal',
        'other_income',
        'other_expense',
    }

    _external_flow_types = {
        'deposit',
        'withdrawal',
        'other_income',
        'other_expense',
    }

    _position_operation_types = {
        'buy',
        'sell',
        'repayment',
    }

    def __init__(self, account: BrokerAccount, price_provider: Optional[PriceProvider] = None):
        self.account = account
        self.price_provider: PriceProvider = price_provider or TransactionPriceProvider()
        if account.provider_type == 'T-Invest_API':
            self.api_service = TInvestService(account)
        else:
            self.api_service = None

    def get_transactions_queryset(self, search_query: str = '', operation_type: str = 'all') -> QuerySet:
        """
        Возвращает queryset операций по счету с применением серверных фильтров.

        Args:
            search_query: Поисковая строка по активу и типу операции.
            operation_type: Тип операции из UI-фильтра.

        Returns:
            QuerySet: Отфильтрованный queryset транзакций.
        """
        queryset = Transaction.objects.filter(account=self.account).select_related('account', 'asset').order_by('-date')
        queryset = self._apply_search_filter(queryset, search_query)
        queryset = self._apply_operation_type_filter(queryset, operation_type)
        return queryset

    def _apply_search_filter(self, queryset: QuerySet, search_query: str) -> QuerySet:
        """Ищет по тикеру, названию актива и типу операции."""
        query = search_query.strip()
        if not query:
            return queryset

        query_lower = query.lower()
        matched_operation_types = [
            code for code, label in Transaction.OPERATION_CHOICES
            if query_lower in code.lower() or query_lower in label.lower()
        ]

        search_filter = (
            Q(asset__ticker__icontains=query) |
            Q(asset__name__icontains=query) |
            Q(operation_type__icontains=query)
        )

        if matched_operation_types:
            search_filter |= Q(operation_type__in=matched_operation_types)

        return queryset.filter(search_filter)

    def _apply_operation_type_filter(self, queryset: QuerySet, operation_type: str) -> QuerySet:
        """Фильтрует список по пользовательскому селектору категорий."""
        normalized_type = operation_type.strip().lower()

        if normalized_type in ('', 'all'):
            return queryset

        if normalized_type == 'buy':
            return queryset.filter(operation_type='buy')
        if normalized_type == 'sell':
            return queryset.filter(operation_type='sell')
        if normalized_type == 'deposit':
            return queryset.filter(operation_type__in=['deposit', 'other_income'])
        if normalized_type == 'withdrawal':
            return queryset.filter(operation_type__in=['withdrawal', 'other_expense'])
        if normalized_type == 'other':
            return queryset.exclude(operation_type__in=self._main_operation_types)

        return queryset

    def _get_transactions_df(self, queryset: Optional[Any] = None) -> pd.DataFrame:
        """Получает транзакции из базы данных и конвертирует их в DataFrame."""
        if queryset is None:
            queryset = Transaction.objects.filter(account=self.account)

        transactions = queryset.values(
            'date', 'operation_type', 'quantity', 'price_per_unit', 'asset__ticker', 'asset__asset_type'
        ).order_by('date')

        if not transactions:
            return pd.DataFrame()

        df = pd.DataFrame(list(transactions))
        # Конвертация типов
        df['date'] = pd.to_datetime(df['date'])
        df['quantity'] = df['quantity'].fillna(Decimal('0'))
        df['price_per_unit'] = df['price_per_unit'].fillna(Decimal('0'))
        df['total_amount'] = df['quantity'] * df['price_per_unit']
        return df

    @staticmethod
    def _to_decimal(value: Any) -> Decimal:
        """Безопасно приводит значение к Decimal для денежных расчетов."""
        if isinstance(value, Decimal):
            return value
        if value is None:
            return Decimal('0')
        return Decimal(str(value))

    @staticmethod
    def _demo_price_multiplier(asset_type: str) -> Decimal:
        """Возвращает демонстрационный множитель цены для ручного портфеля.

        Args:
            asset_type: Тип актива.

        Returns:
            Decimal: Коэффициент для расчета текущей цены.
        """
        multipliers = {
            'share': Decimal('1.12'),
            'bond': Decimal('1.05'),
            'etf': Decimal('1.08'),
            'currency': Decimal('1.00'),
        }
        return multipliers.get(asset_type.lower(), Decimal('1.06'))

    def _build_manual_portfolio_snapshot(self) -> Dict[str, Any]:
        """Собирает локальный снимок портфеля для счетов ручного ввода.

        Returns:
            Dict[str, Any]: Снимок портфеля в формате, совместимом с T-Invest API.
        """
        transactions = Transaction.objects.filter(account=self.account).select_related('asset')
        if not transactions.exists():
            return {
                'total_amount': Decimal('0'),
                'positions': [],
                'currencies': [],
                'updated_at': now(),
            }

        rows = list(
            transactions.values(
                'asset_id',
                'asset__figi',
                'asset__instrument_uid',
                'asset__ticker',
                'asset__name',
                'asset__asset_type',
                'asset__currency',
                'operation_type',
                'quantity',
                'price_per_unit',
                'yield_amount',
                'commission_amount',
                'accrued_int',
            )
        )

        if not rows:
            return {
                'total_amount': Decimal('0'),
                'positions': [],
                'currencies': [],
                'updated_at': now(),
            }

        df = pd.DataFrame(rows)
        df['quantity'] = df['quantity'].fillna(Decimal('0'))
        df['price_per_unit'] = df['price_per_unit'].fillna(Decimal('0'))
        df['amount'] = df['quantity'] * df['price_per_unit']

        cash_signs = {
            'deposit': Decimal('1'),
            'withdrawal': Decimal('-1'),
            'buy': Decimal('-1'),
            'sell': Decimal('1'),
            'repayment': Decimal('1'),
            'dividend': Decimal('1'),
            'coupon': Decimal('1'),
            'amortization': Decimal('1'),
            'other_accrual': Decimal('1'),
            'commission': Decimal('-1'),
            'conversion_commission': Decimal('-1'),
            'tax': Decimal('-1'),
            'tax_refund': Decimal('1'),
            'expense': Decimal('-1'),
            'other_income': Decimal('1'),
            'other_expense': Decimal('-1'),
            'conversion': Decimal('0'),
        }
        df['cash_sign'] = df['operation_type'].map(cash_signs).fillna(Decimal('0'))
        df['cash_delta'] = df['amount'] * df['cash_sign']

        positions: list[dict[str, Any]] = []
        asset_df = df[df['asset_id'].notna() & df['operation_type'].isin(['buy', 'sell'])].copy()

        if not asset_df.empty:
            grouped = asset_df.groupby(
                ['asset_id', 'asset__ticker', 'asset__name', 'asset__asset_type', 'asset__currency'],
                dropna=False,
            )

            for _, group in grouped:
                buy_rows = group[group['operation_type'] == 'buy']
                sell_rows = group[group['operation_type'] == 'sell']

                buy_quantity = self._to_decimal(buy_rows['quantity'].sum())
                sell_quantity = self._to_decimal(sell_rows['quantity'].sum())
                net_quantity = buy_quantity - sell_quantity

                if net_quantity <= 0:
                    continue

                buy_amount = self._to_decimal(buy_rows['amount'].sum())
                average_buy_price = buy_amount / buy_quantity if buy_quantity > 0 else Decimal('0')

                asset_type = str(group.iloc[0]['asset__asset_type'] or '').lower()
                current_price = average_buy_price * self._demo_price_multiplier(asset_type)
                current_value = net_quantity * current_price
                invested = net_quantity * average_buy_price

                positions.append({
                    'figi': group.iloc[0]['asset__figi'] or group.iloc[0]['asset__ticker'],
                    'instrument_uid': group.iloc[0]['asset__instrument_uid'] or group.iloc[0]['asset__ticker'],
                    'ticker': group.iloc[0]['asset__ticker'],
                    'instrument_type': asset_type,
                    'quantity': net_quantity,
                    'average_buy_price': average_buy_price,
                    'current_price': current_price,
                    'expected_yield': current_value - invested,
                    'current_nkd': self._to_decimal(group['accrued_int'].sum()) if asset_type == 'bond' else Decimal('0'),
                })

        cash_balance = self._to_decimal(df['cash_delta'].sum())
        currencies = []
        if cash_balance > 0:
            currencies.append({
                'currency': 'rub',
                'balance': cash_balance,
            })

        total_amount = sum((pos['quantity'] * pos['current_price'] for pos in positions), Decimal('0')) + cash_balance

        return {
            'account_id': self.account.pk,
            'total_amount': total_amount,
            'positions': positions,
            'currencies': currencies,
            'updated_at': now(),
        }

    def get_current_portfolio_snapshot(self) -> Dict[str, Any]:
        """
        Возвращает текущую оценку портфеля.
        Если доступен API Т-Инвестиций, использует его метод get_portfolio().
        Для счетов ручного ввода используется локальный fallback по транзакциям,
        чтобы демо-данные тоже отображались на дашборде.
        """
        if self.api_service:
            try:
                snapshot = self.api_service.get_portfolio()
                return snapshot
            except (TInvestServiceError, ValueError):
                logger.exception('Не удалось получить снимок портфеля через API.')

        return self._build_manual_portfolio_snapshot()

    def get_portfolio_positions(self) -> list:
        """Возвращает список текущих позиций из снимка портфеля."""
        snapshot = self.get_current_portfolio_snapshot()
        return snapshot.get('positions', [])

    def get_cash_balance(self) -> list:
        """Возвращает список валютных активов (кэша) из снимка портфеля."""
        snapshot = self.get_current_portfolio_snapshot()
        return snapshot.get('currencies', [])

    def calculate_xirr(self) -> float:
        """
        Рассчитывает XIRR (внутреннюю норму доходности).
        Использует пополнения (Deposit) и снятия (Withdrawal) как денежные потоки.
        Депозит (инвестиция) идет со знаком минус (уходит на биржу).
        Снятие (возврат) идет со знаком плюс.
        Текущая стоимость портфеля добавляется со знаком плюс (как потенциальное снятие всех средств).
        """
        df = self._get_transactions_df()
        if df.empty:
            return 0.0

        cash_flows_df = df[df['operation_type'].isin(['deposit', 'withdrawal'])].copy()

        if cash_flows_df.empty:
            return 0.0

        cash_flows_df['signed_amount'] = cash_flows_df['total_amount']
        cash_flows_df.loc[cash_flows_df['operation_type'] == 'deposit', 'signed_amount'] *= -1

        dates = cash_flows_df['date'].tolist()
        amounts = cash_flows_df['signed_amount'].tolist()

        snapshot = self.get_current_portfolio_snapshot()
        current_value = self._to_decimal(snapshot.get('total_amount', Decimal('0')))

        # Добавляем текущую стоимость портфеля как финальный поток
        if current_value > 0:
            dates.append(now())
            amounts.append(float(current_value))

        amounts = [float(amount) for amount in amounts]

        try:
            xirr_value = pyxirr.xirr(dates, amounts)
            # Возвращаем значение в процентах (умножаем на 100)
            return float(xirr_value) * 100.0 if xirr_value is not None else 0.0
        except (ValueError, TypeError, ArithmeticError):
            logger.exception('Не удалось рассчитать XIRR для счета id=%s', self.account.pk)
            return 0.0

    def _get_twr_transactions_df(self) -> pd.DataFrame:
        """Возвращает транзакции в формате DataFrame для расчета TWR.

        Returns:
            pd.DataFrame: Данные операций с колонками для денежных потоков и позиций.
        """
        rows = list(
            Transaction.objects.filter(account=self.account)
            .values('id', 'date', 'asset_id', 'operation_type', 'quantity', 'price_per_unit')
            .order_by('date', 'id')
        )
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        df['quantity'] = df['quantity'].fillna(Decimal('0'))
        df['price_per_unit'] = df['price_per_unit'].fillna(Decimal('0'))
        df['amount'] = df['quantity'] * df['price_per_unit']
        return df

    def _build_twr_valuation_dates(self, df: pd.DataFrame) -> pd.DatetimeIndex:
        """Формирует контрольные даты оценки для TWR.

        Args:
            df: Таблица транзакций.

        Returns:
            pd.DatetimeIndex: Отсортированный индекс контрольных дат.
        """
        if df.empty:
            return pd.DatetimeIndex([])

        start_date = df['date'].min()
        end_date = max(pd.Timestamp(now()), df['date'].max())
        flow_dates = df.loc[df['operation_type'].isin(self._external_flow_types), 'date']

        all_points = pd.DatetimeIndex([start_date, end_date]).append(pd.DatetimeIndex(flow_dates.tolist()))
        return pd.DatetimeIndex(sorted(pd.Series(all_points).dropna().unique()))

    def _build_holdings_matrix(self, df: pd.DataFrame, valuation_dates: pd.DatetimeIndex) -> pd.DataFrame:
        """Строит матрицу количеств позиций [дата x asset_id].

        Args:
            df: Таблица транзакций.
            valuation_dates: Контрольные даты оценки.

        Returns:
            pd.DataFrame: Кумулятивные позиции по активам на даты оценки.
        """
        if valuation_dates.empty:
            return pd.DataFrame()

        position_df = df[
            df['asset_id'].notna() &
            df['operation_type'].isin(self._position_operation_types)
        ].copy()

        if position_df.empty:
            return pd.DataFrame(index=valuation_dates)

        qty_signs = {
            'buy': Decimal('1'),
            'sell': Decimal('-1'),
            'repayment': Decimal('-1'),
        }
        position_df['signed_quantity'] = position_df['quantity'] * position_df['operation_type'].map(qty_signs)

        grouped = (
            position_df
            .groupby(['date', 'asset_id'], as_index=False)['signed_quantity']
            .sum()
        )

        pivot = grouped.pivot(index='date', columns='asset_id', values='signed_quantity').sort_index()
        all_dates = valuation_dates.union(pivot.index)
        holdings = (
            pivot
            .reindex(all_dates)
            .fillna(Decimal('0'))
            .cumsum()
            .reindex(valuation_dates)
            .fillna(Decimal('0'))
        )
        return holdings

    def _build_cash_series(self, df: pd.DataFrame, valuation_dates: pd.DatetimeIndex) -> pd.Series:
        """Строит кумулятивный денежный баланс на даты оценки.

        Args:
            df: Таблица транзакций.
            valuation_dates: Контрольные даты оценки.

        Returns:
            pd.Series: Денежный баланс счета на каждую дату оценки.
        """
        cash_signs = {
            'deposit': Decimal('1'),
            'withdrawal': Decimal('-1'),
            'buy': Decimal('-1'),
            'sell': Decimal('1'),
            'repayment': Decimal('1'),
            'dividend': Decimal('1'),
            'coupon': Decimal('1'),
            'amortization': Decimal('1'),
            'other_accrual': Decimal('1'),
            'commission': Decimal('-1'),
            'conversion_commission': Decimal('-1'),
            'tax': Decimal('-1'),
            'tax_refund': Decimal('1'),
            'expense': Decimal('-1'),
            'other_income': Decimal('1'),
            'other_expense': Decimal('-1'),
            'conversion': Decimal('0'),
        }
        cash_df = df.copy()
        cash_df['cash_delta'] = cash_df['amount'] * cash_df['operation_type'].map(cash_signs).fillna(Decimal('0'))
        by_date = cash_df.groupby('date', as_index=True)['cash_delta'].sum().sort_index()

        all_dates = valuation_dates.union(by_date.index)
        return (
            by_date
            .reindex(all_dates)
            .fillna(Decimal('0'))
            .cumsum()
            .reindex(valuation_dates)
            .fillna(Decimal('0'))
        )

    def _build_external_flows_series(self, df: pd.DataFrame, valuation_dates: pd.DatetimeIndex) -> pd.Series:
        """Считает внешние денежные потоки между контрольными датами.

        Args:
            df: Таблица транзакций.
            valuation_dates: Контрольные даты оценки.

        Returns:
            pd.Series: Потоки по периодам с индексом valuation_dates.
        """
        ext_signs = {
            'deposit': Decimal('1'),
            'other_income': Decimal('1'),
            'withdrawal': Decimal('-1'),
            'other_expense': Decimal('-1'),
        }
        ext_df = df[df['operation_type'].isin(self._external_flow_types)].copy()
        if ext_df.empty:
            return pd.Series(Decimal('0'), index=valuation_dates)

        ext_df['external_amount'] = ext_df['amount'] * ext_df['operation_type'].map(ext_signs)
        by_date = ext_df.groupby('date', as_index=True)['external_amount'].sum().sort_index()
        all_dates = valuation_dates.union(by_date.index)
        cumulative = (
            by_date
            .reindex(all_dates)
            .fillna(Decimal('0'))
            .cumsum()
            .reindex(valuation_dates)
            .fillna(Decimal('0'))
        )
        return cumulative.diff().fillna(Decimal('0'))

    def calculate_twr(self) -> Optional[float]:
        """Рассчитывает TWR (Time-Weighted Return) в процентах.

        Формула по периодам:
            r_i = (V_i - V_{i-1} - CF_i) / V_{i-1}
            TWR = (Π(1 + r_i) - 1) * 100

        Где:
            V_i — стоимость портфеля в конце периода,
            CF_i — внешний поток средств в периоде (пополнения/выводы).

        Returns:
            Optional[float]: TWR в процентах. Возвращает 0.0, если данных недостаточно.
        """
        df = self._get_twr_transactions_df()
        if df.empty:
            return 0.0

        valuation_dates = self._build_twr_valuation_dates(df)
        if len(valuation_dates) < 2:
            return 0.0

        holdings = self._build_holdings_matrix(df, valuation_dates)
        cash_series = self._build_cash_series(df, valuation_dates)

        if holdings.empty:
            positions_value = pd.Series(Decimal('0'), index=valuation_dates)
        else:
            asset_ids = [int(asset_id) for asset_id in holdings.columns.tolist()]
            price_matrix = self.price_provider.get_price_matrix(self.account, asset_ids, valuation_dates)
            aligned_prices = (
                price_matrix
                .reindex(index=valuation_dates)
                .reindex(columns=holdings.columns)
                .fillna(Decimal('0'))
            )
            positions_value = (holdings * aligned_prices).sum(axis=1)

        portfolio_values = positions_value + cash_series
        external_flows = self._build_external_flows_series(df, valuation_dates)

        prev_values = portfolio_values.shift(1)
        period_returns = (portfolio_values - prev_values - external_flows) / prev_values
        valid_mask = prev_values > 0
        period_returns = period_returns.where(valid_mask).dropna()

        if period_returns.empty:
            return 0.0

        gross = period_returns.apply(lambda value: Decimal('1') + self._to_decimal(value))
        twr = gross.prod() - Decimal('1')
        return float(twr * Decimal('100'))

    def get_allocation_data(self) -> tuple[list[AllocationGroup], list[str], list[float]]:
        """
        Возвращает данные для круговой диаграммы и таблицы классов активов:
        (portfolio_classes, labels, values)
        """
        asset_classes = {
            'share': 'Акции',
            'bond': 'Облигации',
            'etf': 'Фонды',
            'currency': 'Валюта',
            'crypto': 'Криптовалюта',
        }

        groups: dict[str, AllocationGroup] = {
            name: {
                'name': name,
                'current_value': Decimal('0'),
                'invested': Decimal('0'),
                'yield_amount': Decimal('0'),
                'share': Decimal('0'),
                'yield_percent': Decimal('0'),
            }
            for name in asset_classes.values()
        }

        positions = self.get_portfolio_positions()
        for pos in positions:
            itype = str(pos.get('instrument_type', '')).lower()
            class_name = asset_classes.get(itype, 'Прочее')
            if class_name not in groups:
                groups[class_name] = {
                    'name': class_name,
                    'current_value': Decimal('0'),
                    'invested': Decimal('0'),
                    'yield_amount': Decimal('0'),
                    'share': Decimal('0'),
                    'yield_percent': Decimal('0'),
                }

            qty = self._to_decimal(pos.get('quantity'))
            price = self._to_decimal(pos.get('current_price'))
            avg = self._to_decimal(pos.get('average_buy_price'))
            yld = self._to_decimal(pos.get('expected_yield'))

            groups[class_name]['current_value'] += qty * price
            groups[class_name]['invested'] += qty * avg
            groups[class_name]['yield_amount'] += yld

        currencies = self.get_cash_balance()
        for cur in currencies:
            bal = self._to_decimal(cur.get('balance'))
            groups['Валюта']['current_value'] += bal
            groups['Валюта']['invested'] += bal

        total_portfolio_calc = sum((g['current_value'] for g in groups.values()), Decimal('0'))

        labels = []
        values = []
        portfolio_classes = []

        for g in groups.values():
            cv = g['current_value']
            if cv > 0 or g['invested'] > 0:
                labels.append(g['name'])
                values.append(float(cv))

                g['share'] = (cv / total_portfolio_calc * Decimal('100')) if total_portfolio_calc > 0 else Decimal('0')
                g['yield_percent'] = (
                    g['yield_amount'] / g['invested'] * Decimal('100')
                ) if g['invested'] > 0 else Decimal('0')
                portfolio_classes.append(g)

        portfolio_classes.sort(key=lambda x: x['share'], reverse=True)
        return portfolio_classes, labels, values

    def _calculate_position_metrics(self, metrics: dict) -> None:
        """Считает метрики на основе текущих позиций (nkd, ожидаемая доходность)"""
        positions = self.get_portfolio_positions()
        for pos in positions:
            yield_val = self._to_decimal(pos.get('expected_yield'))
            nkd_val = self._to_decimal(pos.get('current_nkd'))
            metrics['asset_price_difference'] += yield_val
            metrics['aci'] += nkd_val

    def _calculate_transaction_metrics(self, metrics: dict) -> None:
        """Считает метрики на основе исторических транзакций"""
        transactions = Transaction.objects.filter(account=self.account)
        stats = transactions.values('operation_type').annotate(
            total_sum=Sum(F('quantity') * F('price_per_unit')),
            total_yield=Sum('yield_amount'),
            total_aci=Sum('accrued_int')
        )

        accrual_types = ('dividend', 'coupon', 'amortization', 'other_accrual')
        tax_types = ('tax', 'tax_refund')
        comm_types = ('commission', 'conversion_commission')

        for stat in stats:
            op_type = stat['operation_type']
            total_sum = self._to_decimal(stat['total_sum'])

            # Реализованная прибыль (от продаж/погашений)
            metrics['realized_pnl'] += self._to_decimal(stat['total_yield'])

            # Исторический НКД: платим (-), получаем (+)
            aci_val = self._to_decimal(stat['total_aci'])
            if op_type == 'buy':
                metrics['aci'] -= aci_val
            elif op_type in ('sell', 'repayment'):
                metrics['aci'] += aci_val

            if op_type in accrual_types:
                metrics['accruals'] += total_sum
            elif op_type in tax_types:
                # Учитываем tax_refund как уменьшение налогов
                if op_type == 'tax':
                    metrics['taxes'] += total_sum
                else:
                    metrics['taxes'] -= total_sum
            elif op_type in comm_types:
                metrics['commissions'] += total_sum

    def get_profit_metrics(self) -> dict:
        """
        Агрегирует данные о прибыли по категориям:
        Разница цен, начисления, налоги, комиссии, НКД, а также прибыль с продаж.
        """
        metrics = {
            'asset_price_difference': Decimal('0'),
            'realized_pnl': Decimal('0'),
            'accruals': Decimal('0'),
            'taxes': Decimal('0'),
            'commissions': Decimal('0'),
            'aci': Decimal('0'),
            'total_profit': Decimal('0')
        }

        self._calculate_position_metrics(metrics)
        self._calculate_transaction_metrics(metrics)

        # Общая прибыль: Разница цен + Зафиксированная прибыль + Начисления + НКД - Налоги - Комиссии
        metrics['total_profit'] = (
            metrics['asset_price_difference'] +
            metrics['realized_pnl'] +
            metrics['accruals'] +
            metrics['aci'] -
            metrics['taxes'] -
            metrics['commissions']
        )
        return metrics

    def get_portfolio_cash_flows(self, end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Возвращает DataFrame всех вводов/выводов средств с датами
        """
        transactions = Transaction.objects.filter(account=self.account)
        if end_date:
            transactions = transactions.filter(date__lte=end_date)

        df = self._get_transactions_df(transactions)

        if df.empty:
            return pd.DataFrame(columns=['date', 'amount'])

        # Фильтруем только пополнения и выводы
        cash_flows_df = df[df['operation_type'].isin(['deposit', 'withdrawal'])].copy()

        if cash_flows_df.empty:
            return pd.DataFrame(columns=['date', 'amount'])

        cash_flows_df['amount'] = cash_flows_df['total_amount']
        cash_flows_df.loc[cash_flows_df['operation_type'] == 'withdrawal', 'amount'] *= -1

        return cash_flows_df[['date', 'amount']]

    @staticmethod
    def get_category_totals(queryset) -> dict[str, str]:
        """
        Агрегирует операции по 7 основным категориям для дэшборда/истории операций.
        Использует БД для подсчета (O(1) по передаче данных).
        """
        from django.db.models import Sum, F, Case, When, DecimalField

        totals = queryset.aggregate(
            buy_sum=Sum(
                Case(
                    When(operation_type='buy', then=F('quantity') * F('price_per_unit')),
                    default=0,
                    output_field=DecimalField()
                )
            ),
            sell_sum=Sum(
                Case(
                    When(operation_type='sell', then=F('quantity') * F('price_per_unit')),
                    default=0,
                    output_field=DecimalField()
                )
            ),
            commission_sum=Sum(
                Case(
                    When(operation_type__in=['commission', 'conversion_commission'], then=F('quantity') * F('price_per_unit')),
                    default=0,
                    output_field=DecimalField()
                )
            ),
            accrual_sum=Sum(
                Case(
                    When(operation_type__in=['dividend', 'coupon', 'amortization', 'other_accrual'], then=F('quantity') * F('price_per_unit')),
                    default=0,
                    output_field=DecimalField()
                )
            ),
            tax_sum=Sum(
                Case(
                    When(operation_type__in=['tax', 'tax_refund'], then=F('quantity') * F('price_per_unit')),
                    default=0,
                    output_field=DecimalField()
                )
            ),
            deposit_sum=Sum(
                Case(
                    When(operation_type__in=['deposit', 'other_income'], then=F('quantity') * F('price_per_unit')),
                    default=0,
                    output_field=DecimalField()
                )
            ),
            withdrawal_sum=Sum(
                Case(
                    When(operation_type__in=['withdrawal', 'other_expense'], then=F('quantity') * F('price_per_unit')),
                    default=0,
                    output_field=DecimalField()
                )
            )
        )

        return {
            'buy': f"{float(totals['buy_sum'] or 0):.2f}",
            'sell': f"{float(totals['sell_sum'] or 0):.2f}",
            'commission': f"{float(totals['commission_sum'] or 0):.2f}",
            'accrual': f"{float(totals['accrual_sum'] or 0):.2f}",
            'tax': f"{float(totals['tax_sum'] or 0):.2f}",
            'deposit': f"{float(totals['deposit_sum'] or 0):.2f}",
            'withdrawal': f"{float(totals['withdrawal_sum'] or 0):.2f}"
        }

