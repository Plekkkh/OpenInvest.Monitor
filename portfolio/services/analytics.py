import pandas as pd
import pyxirr
import logging
from typing import Dict, Any, Optional, TypedDict
from datetime import datetime

from django.utils.timezone import now
from django.db.models import Sum, F
from portfolio.models import BrokerAccount, Transaction
from portfolio.services.t_invest import TInvestService, TInvestServiceError


logger = logging.getLogger(__name__)


class AllocationGroup(TypedDict):
    name: str
    current_value: float
    invested: float
    yield_amount: float
    share: float
    yield_percent: float


class AnalyticsService:
    def __init__(self, account: BrokerAccount):
        self.account = account
        if account.provider_type == 'T-Invest_API':
            self.api_service = TInvestService(account)
        else:
            self.api_service = None

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
        df['quantity'] = df['quantity'].astype(float)
        df['price_per_unit'] = df['price_per_unit'].astype(float)
        df['total_amount'] = df['quantity'] * df['price_per_unit']
        return df

    def get_current_portfolio_snapshot(self) -> Dict[str, Any]:
        """
        Возвращает текущую оценку портфеля.
        Если доступен API Т-Инвестиций, использует его метод get_portfolio().
        Для счетов ручного ввода используется fallback-механизм (пока возвращает 0,
        в будущем можно реализовать расчет по базе).
        """
        if self.api_service:
            try:
                snapshot = self.api_service.get_portfolio()
                return snapshot
            except (TInvestServiceError, ValueError):
                logger.exception('Не удалось получить снимок портфеля через API.')

        # Fallback для ручных счетов (Manual) - пока упрощенный, вернем 0
        # Для полноценного fallback потребуется запрашивать актуальные цены с рынка
        return {
            'total_amount': 0.0,
            'positions': [],
            'currencies': [],
            'updated_at': now()
        }

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
        current_value = snapshot.get('total_amount', 0.0)

        # Добавляем текущую стоимость портфеля как финальный поток
        if current_value > 0:
            dates.append(now())
            amounts.append(current_value)

        try:
            xirr_value = pyxirr.xirr(dates, amounts)
            # Возвращаем значение в процентах (умножаем на 100)
            return float(xirr_value) * 100.0 if xirr_value is not None else 0.0
        except (ValueError, TypeError, ArithmeticError):
            logger.exception('Не удалось рассчитать XIRR для счета id=%s', self.account.id)
            return 0.0

    def calculate_twr(self) -> Optional[float]:
        """
        Упрощенный расчет TWR отложен до реализации подкачки исторических котировок (market data).

        TODO
        """
        return None

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
                'current_value': 0.0,
                'invested': 0.0,
                'yield_amount': 0.0,
                'share': 0.0,
                'yield_percent': 0.0,
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
                    'current_value': 0.0,
                    'invested': 0.0,
                    'yield_amount': 0.0,
                    'share': 0.0,
                    'yield_percent': 0.0,
                }

            qty = float(pos.get('quantity') or 0)
            price = float(pos.get('current_price') or 0)
            avg = float(pos.get('average_buy_price') or 0)
            yld = float(pos.get('expected_yield') or 0)

            groups[class_name]['current_value'] += qty * price
            groups[class_name]['invested'] += qty * avg
            groups[class_name]['yield_amount'] += yld

        currencies = self.get_cash_balance()
        for cur in currencies:
            bal = float(cur.get('balance') or 0)
            groups['Валюта']['current_value'] += bal
            groups['Валюта']['invested'] += bal

        total_portfolio_calc: float = sum(float(g['current_value']) for g in groups.values())

        labels = []
        values = []
        portfolio_classes = []

        for g in groups.values():
            cv = g['current_value']
            if cv > 0 or g['invested'] > 0:
                labels.append(g['name'])
                values.append(cv)

                g['share'] = (cv / total_portfolio_calc * 100) if total_portfolio_calc > 0 else 0.0
                g['yield_percent'] = (g['yield_amount'] / g['invested'] * 100) if g['invested'] > 0 else 0.0
                portfolio_classes.append(g)

        portfolio_classes.sort(key=lambda x: x['share'], reverse=True)
        return portfolio_classes, labels, values

    def _calculate_position_metrics(self, metrics: dict) -> None:
        """Считает метрики на основе текущих позиций (nkd, ожидаемая доходность)"""
        positions = self.get_portfolio_positions()
        for pos in positions:
            yield_val = pos.get('expected_yield') or 0.0
            nkd_val = pos.get('current_nkd') or 0.0
            metrics['asset_price_difference'] += float(yield_val)
            metrics['aci'] += float(nkd_val)

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
            total_sum = float(stat['total_sum'] or 0)

            # Реализованная прибыль (от продаж/погашений)
            metrics['realized_pnl'] += float(stat['total_yield'] or 0)

            # Исторический НКД: платим (-), получаем (+)
            aci_val = float(stat['total_aci'] or 0)
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
            'asset_price_difference': 0.0,
            'realized_pnl': 0.0,
            'accruals': 0.0,
            'taxes': 0.0,
            'commissions': 0.0,
            'aci': 0.0,
            'total_profit': 0.0
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
