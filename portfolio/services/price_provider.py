from __future__ import annotations

from decimal import Decimal
from typing import Protocol

import pandas as pd

from portfolio.models import BrokerAccount, Transaction


class PriceProvider(Protocol):
    """Интерфейс источника цен для аналитики.

    Реализации могут брать цены из API брокера, локального кеша или БД.
    """

    def get_price_matrix(
        self,
        account: BrokerAccount,
        asset_ids: list[int],
        valuation_dates: pd.DatetimeIndex,
    ) -> pd.DataFrame:
        """Возвращает матрицу цен [дата x asset_id].

        Args:
            account: Брокерский счет.
            asset_ids: Идентификаторы активов.
            valuation_dates: Даты, в которые нужно оценить активы.

        Returns:
            pd.DataFrame: Матрица цен с индексом valuation_dates и колонками asset_ids.
        """


class TransactionPriceProvider:
    """Провайдер цен на основе последних цен сделок.

    Используется как безопасный fallback для ручных счетов и тестовых данных,
    когда полноценная историческая market data недоступна.
    """

    _price_operation_types = ('buy', 'sell', 'repayment')

    def get_price_matrix(
        self,
        account: BrokerAccount,
        asset_ids: list[int],
        valuation_dates: pd.DatetimeIndex,
    ) -> pd.DataFrame:
        """Строит матрицу цен по последней известной цене сделки.

        Args:
            account: Брокерский счет.
            asset_ids: Идентификаторы активов.
            valuation_dates: Даты оценки.

        Returns:
            pd.DataFrame: Матрица цен [дата x asset_id].
        """
        if valuation_dates.empty:
            return pd.DataFrame()

        if not asset_ids:
            return pd.DataFrame(index=valuation_dates)

        price_rows = list(
            Transaction.objects.filter(
                account=account,
                asset_id__in=asset_ids,
                operation_type__in=self._price_operation_types,
                date__lte=valuation_dates.max().to_pydatetime(),
            )
            .values('id', 'date', 'asset_id', 'price_per_unit')
            .order_by('date', 'id')
        )

        if not price_rows:
            return pd.DataFrame(Decimal('0'), index=valuation_dates, columns=asset_ids)

        prices_df = pd.DataFrame(price_rows)
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        prices_df['price_per_unit'] = prices_df['price_per_unit'].fillna(Decimal('0'))
        prices_df = prices_df[prices_df['price_per_unit'] > 0]

        if prices_df.empty:
            return pd.DataFrame(Decimal('0'), index=valuation_dates, columns=asset_ids)

        # Последняя цена сделки по активу на конкретную дату.
        by_date_asset = (
            prices_df
            .sort_values(['date', 'id'])
            .groupby(['date', 'asset_id'], as_index=False)
            .last()[['date', 'asset_id', 'price_per_unit']]
        )

        pivot = by_date_asset.pivot(index='date', columns='asset_id', values='price_per_unit').sort_index()

        all_dates = valuation_dates.union(pivot.index)
        matrix = (
            pivot
            .reindex(all_dates)
            .sort_index()
            .ffill()
            .reindex(valuation_dates)
            .reindex(columns=asset_ids)
            .fillna(Decimal('0'))
        )
        return matrix
