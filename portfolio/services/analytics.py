import pandas as pd
import pyxirr
from typing import Dict, Any, Optional
from datetime import datetime

from django.utils.timezone import now
from portfolio.models import BrokerAccount, Transaction
from portfolio.services.t_invest import TInvestService

class AnalyticsService:
    def __init__(self, account: BrokerAccount):
        self.account = account
        if account.provider_type == 'T-Invest_API':
            self.api_service = TInvestService(account)
        else:
            self.api_service = None

    def _get_transactions_df(self) -> pd.DataFrame:
        """Получает транзакции из базы данных и конвертирует их в DataFrame."""
        transactions = Transaction.objects.filter(account=self.account).values(
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
            except Exception:
                # В случае ошибки API возвращаем пустой снимок
                pass
        
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

        dates = cash_flows_df['date'].tolist()
        amounts = []
        for _, row in cash_flows_df.iterrows():
            if row['operation_type'] == 'deposit':
                amounts.append(-row['total_amount'])
            else:
                amounts.append(row['total_amount'])

        snapshot = self.get_current_portfolio_snapshot()
        current_value = snapshot.get('total_amount', 0.0)

        # Добавляем текущую стоимость портфеля как финальный положительный поток
        if current_value > 0:
            dates.append(now())
            amounts.append(current_value)

        try:
            xirr_value = pyxirr.xirr(dates, amounts)
            # Возвращаем значение в процентах (умножаем на 100)
            return float(xirr_value) * 100.0 if xirr_value is not None else 0.0
        except Exception:
            return 0.0

    def calculate_twr(self) -> Optional[float]:
        """
        Упрощенный расчет TWR отложен до реализации подкачки исторических котировок (market data).

        TODO
        """
        return None

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

        # 1. Разница цены активов и НКД из текущих позиций
        positions = self.get_portfolio_positions()
        for pos in positions:
            metrics['asset_price_difference'] += float(pos.get('expected_yield') or 0.0)
            metrics['aci'] += float(pos.get('current_nkd') or 0.0)

        # 2. Начисления, Налоги, Комиссии, Исторический НКД из списка транзакций
        from django.db.models import Sum, F
        transactions = Transaction.objects.filter(account=self.account)
        stats = transactions.values('operation_type').annotate(
            total_sum=Sum(F('quantity') * F('price_per_unit')),
            total_yield=Sum('yield_amount'),
            total_aci=Sum('accrued_int')
        )

        for stat in stats:
            op_type = stat['operation_type']
            total_sum = float(stat['total_sum'] or 0)

            # Реализованная прибыль (приходит готовой от брокера при продажах/погашениях)
            metrics['realized_pnl'] += float(stat['total_yield'] or 0)

            # Исторический НКД при сделках: при покупке мы его платим (-), при продаже/погашении получаем (+)
            aci_val = float(stat['total_aci'] or 0)
            if op_type == 'buy':
                metrics['aci'] -= aci_val
            elif op_type in ('sell', 'repayment'):
                metrics['aci'] += aci_val

            if op_type == 'dividend' or op_type == 'coupon' or op_type == 'amortization' or op_type == 'other_accrual':
                metrics['accruals'] += total_sum
            elif op_type == 'tax' or op_type == 'tax_refund':
                # Учитываем tax_refund как уменьшение налогов
                if op_type == 'tax':
                    metrics['taxes'] += total_sum
                else:
                    metrics['taxes'] -= total_sum
            elif op_type == 'commission' or op_type == 'conversion_commission':
                metrics['commissions'] += total_sum

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

    def get_portfolio_cash_flows(self, end_date: datetime = None) -> pd.DataFrame:
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

        # amount = price_per_unit * quantity (штук как правило 1, а цена - это сумма)
        # Для вводов (Deposit) значение положительное, для выводов (Withdrawal) - отрицательное
        def get_signed_amount(row):
            total = float(row['price_per_unit']) * float(row['quantity'])
            if row['operation_type'] == 'deposit':
                return total
            return -total

        cash_flows_df['amount'] = cash_flows_df.apply(get_signed_amount, axis=1)

        return cash_flows_df[['date', 'amount']]
