from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any

from django.contrib.auth.models import User
from django.db import transaction as db_transaction
from django.utils.text import slugify
from django.utils.timezone import now

from portfolio.models import Asset, BrokerAccount, Transaction


@dataclass(frozen=True)
class DemoPortfolioResult:
    """Результат создания или обновления демо-портфеля.

    Attributes:
        account: Брокерский счет с демо-данными.
        user_created: Был ли пользователь создан впервые.
        account_created: Был ли счет создан впервые.
        created_transactions: Количество созданных транзакций.
    """

    account: BrokerAccount
    user_created: bool
    account_created: bool
    created_transactions: int


class DemoPortfolioService:
    """Сервис для создания демонстрационного портфеля."""

    default_account_name = 'Test_Inv'
    default_provider_account_id = 'demo-test-inv'

    _asset_specs: list[dict[str, str]] = [
        {
            'key': 'SBER',
            'instrument_uid': 'demo-uid-sber',
            'figi': 'demo-figi-sber',
            'ticker': 'SBER',
            'isin': 'RU0009029540',
            'name': 'Сбербанк',
            'asset_type': 'Share',
            'currency': 'RUB',
        },
        {
            'key': 'OFZ26238',
            'instrument_uid': 'demo-uid-ofz-26238',
            'figi': 'demo-figi-ofz-26238',
            'ticker': 'OFZ26238',
            'isin': 'RU000A100HG8',
            'name': 'ОФЗ 26238',
            'asset_type': 'Bond',
            'currency': 'RUB',
        },
        {
            'key': 'TMOS',
            'instrument_uid': 'demo-uid-tmos',
            'figi': 'demo-figi-tmos',
            'ticker': 'TMOS',
            'isin': 'RU000A1025V3',
            'name': 'Т-iMOEX',
            'asset_type': 'ETF',
            'currency': 'RUB',
        },
    ]

    def seed_for_user(
        self,
        user: User,
        account_name: str | None = None,
        provider_account_id: str | None = None,
    ) -> DemoPortfolioResult:
        """Создает или обновляет демо-данные для указанного пользователя.

        Args:
            user: Пользователь, для которого создается демо-портфель.
            account_name: Название демо-счета.
            provider_account_id: Внешний идентификатор счета у провайдера.

        Returns:
            DemoPortfolioResult: Информация о созданных данных.
        """
        account_name = account_name or self.default_account_name
        provider_account_id = self._build_provider_account_id(
            user=user,
            account_name=account_name,
            provider_account_id=provider_account_id,
        )

        with db_transaction.atomic():
            user, user_created = User.objects.get_or_create(username=user.username)
            account, account_created = BrokerAccount.objects.update_or_create(
                user=user,
                name=account_name,
                defaults={
                    'provider_type': 'Manual',
                    'provider_account_id': provider_account_id,
                },
            )
            account.api_token = None
            account.save(update_fields=['_encrypted_token'])

            assets = self._seed_assets()
            created_transactions = self._seed_transactions(account, assets)

        return DemoPortfolioResult(
            account=account,
            user_created=user_created,
            account_created=account_created,
            created_transactions=created_transactions,
        )

    def _seed_assets(self) -> dict[str, Asset]:
        """Создает или обновляет демонстрационные активы.

        Returns:
            dict[str, Asset]: Словарь активов по ключу.
        """
        assets: dict[str, Asset] = {}

        for spec in self._asset_specs:
            asset, _ = Asset.objects.update_or_create(
                instrument_uid=spec['instrument_uid'],
                defaults={
                    'figi': spec['figi'],
                    'ticker': spec['ticker'],
                    'isin': spec['isin'],
                    'name': spec['name'],
                    'asset_type': spec['asset_type'],
                    'currency': spec['currency'],
                },
            )
            assets[spec['key']] = asset

        return assets

    def _seed_transactions(self, account: BrokerAccount, assets: dict[str, Asset]) -> int:
        """Создает или обновляет демонстрационные транзакции.

        Args:
            account: Брокерский счет.
            assets: Словарь активов.

        Returns:
            int: Количество созданных транзакций.
        """
        base_date = now()
        external_id_prefix = self._build_external_id_prefix(account)
        transaction_specs: list[dict[str, Any]] = [
            {
                'external_id': f'{external_id_prefix}-deposit-1',
                'asset': None,
                'operation_type': 'deposit',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('200000'),
                'date': base_date - timedelta(days=40),
            },
            {
                'external_id': f'{external_id_prefix}-sber-buy',
                'asset': assets['SBER'],
                'operation_type': 'buy',
                'quantity': Decimal('200'),
                'price_per_unit': Decimal('250'),
                'date': base_date - timedelta(days=35),
            },
            {
                'external_id': f'{external_id_prefix}-sber-commission',
                'asset': None,
                'operation_type': 'commission',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('300'),
                'date': base_date - timedelta(days=35),
            },
            {
                'external_id': f'{external_id_prefix}-ofz-buy',
                'asset': assets['OFZ26238'],
                'operation_type': 'buy',
                'quantity': Decimal('60'),
                'price_per_unit': Decimal('1000'),
                'date': base_date - timedelta(days=30),
                'accrued_int': Decimal('150'),
            },
            {
                'external_id': f'{external_id_prefix}-tmos-buy',
                'asset': assets['TMOS'],
                'operation_type': 'buy',
                'quantity': Decimal('40'),
                'price_per_unit': Decimal('1000'),
                'date': base_date - timedelta(days=25),
            },
            {
                'external_id': f'{external_id_prefix}-sber-dividend',
                'asset': assets['SBER'],
                'operation_type': 'dividend',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('600'),
                'date': base_date - timedelta(days=20),
            },
            {
                'external_id': f'{external_id_prefix}-ofz-coupon',
                'asset': assets['OFZ26238'],
                'operation_type': 'coupon',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('1800'),
                'date': base_date - timedelta(days=15),
            },
            {
                'external_id': f'{external_id_prefix}-sber-sell',
                'asset': assets['SBER'],
                'operation_type': 'sell',
                'quantity': Decimal('50'),
                'price_per_unit': Decimal('280'),
                'date': base_date - timedelta(days=10),
                'yield_amount': Decimal('1500'),
            },
            {
                'external_id': f'{external_id_prefix}-tax',
                'asset': None,
                'operation_type': 'tax',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('500'),
                'date': base_date - timedelta(days=7),
            },
            {
                'external_id': f'{external_id_prefix}-withdrawal',
                'asset': None,
                'operation_type': 'withdrawal',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('10000'),
                'date': base_date - timedelta(days=3),
            },
        ]

        created_count = 0
        for spec in transaction_specs:
            _, created = Transaction.objects.update_or_create(
                external_id=spec['external_id'],
                defaults={
                    'account': account,
                    'asset': spec['asset'],
                    'operation_type': spec['operation_type'],
                    'quantity': spec['quantity'],
                    'price_per_unit': spec['price_per_unit'],
                    'date': spec['date'],
                    'yield_amount': spec.get('yield_amount', Decimal('0')),
                    'commission_amount': spec.get('commission_amount', Decimal('0')),
                    'accrued_int': spec.get('accrued_int', Decimal('0')),
                },
            )
            if created:
                created_count += 1

        return created_count

    @staticmethod
    def _build_external_id_prefix(account: BrokerAccount) -> str:
        """Строит префикс для внешних ID демо-транзакций.

        Args:
            account: Брокерский счет.

        Returns:
            str: Стабильный префикс для идентификаторов.
        """
        user_part = slugify(account.user.username) or f'user-{account.user.pk}'
        account_part = slugify(account.name) or f'account-{account.pk}'
        return f'demo-{user_part}-{account_part}'

    def _build_provider_account_id(
        self,
        user: User,
        account_name: str,
        provider_account_id: str | None,
    ) -> str:
        """Возвращает уникальный ID счета у провайдера.

        Args:
            user: Пользователь, для которого создается демо-счет.
            account_name: Название счета.
            provider_account_id: Явно переданный ID провайдера.

        Returns:
            str: Уникальный идентификатор счета у провайдера.
        """
        if provider_account_id:
            return provider_account_id

        user_part = slugify(user.username) or f'user-{user.pk}'
        account_part = slugify(account_name) or 'account'
        return f'demo-{user_part}-{account_part}'

