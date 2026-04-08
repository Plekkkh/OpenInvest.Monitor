import pandas as pd
import pyxirr
from typing import Dict, Any, Optional

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

        cash_flows_df = df[df['operation_type'].isin(['Deposit', 'Withdrawal'])].copy()
        
        if cash_flows_df.empty:
            return 0.0

        dates = cash_flows_df['date'].tolist()
        amounts = []
        for _, row in cash_flows_df.iterrows():
            if row['operation_type'] == 'Deposit':
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
            return float(xirr_value) if xirr_value is not None else 0.0
        except Exception:
            return 0.0

    def calculate_twr(self) -> Optional[float]:
        """
        Упрощенный расчет TWR отложен до реализации подкачки исторических котировок (market data).

        TODO
        """
        return None

