from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

from django.contrib.auth.models import User
from django.core.management.base import BaseCommand
from django.db import transaction as db_transaction
from django.utils.timezone import now

from portfolio.models import Asset, BrokerAccount, Transaction


class Command(BaseCommand):
    """Создает демонстрационные активы и транзакции для дашборда."""

    help = 'Создает демо-пользователя, счет, активы и транзакции для заполнения дашборда.'

    def add_arguments(self, parser: Any) -> None:
        """Добавляет аргументы командной строки.

        Args:
            parser: Парсер аргументов Django.
        """
        parser.add_argument('--username', default='123', help='Имя пользователя для демо-данных')
        parser.add_argument('--account-name', default='Test_Inv', help='Название брокерского счета')
        parser.add_argument(
            '--provider-account-id',
            default='demo-test-inv-123',
            help='Уникальный ID счета у провайдера для демо-данных',
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Создает или обновляет демонстрационный набор данных.

        Args:
            *args: Позиционные аргументы Django.
            **options: Опции командной строки.
        """
        username = options['username']
        account_name = options['account_name']
        provider_account_id = options['provider_account_id']

        with db_transaction.atomic():
            user, user_created = User.objects.get_or_create(username=username)
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

        self.stdout.write(
            self.style.SUCCESS(
                f"Демо-данные готовы: пользователь '{user.username}'{self._format_created(user_created)}, "
                f"счет '{account.name}'{self._format_created(account_created)}, "
                f"транзакций создано/обновлено: {created_transactions}."
            )
        )

    @staticmethod
    def _format_created(created: bool) -> str:
        """Возвращает короткую пометку о создании объекта.

        Args:
            created: Был ли объект создан впервые.

        Returns:
            str: Текстовая пометка.
        """
        return ' (создан)' if created else ' (обновлен)'

    def _seed_assets(self) -> dict[str, Asset]:
        """Создает демонстрационные активы.

        Returns:
            dict[str, Asset]: Словарь активов по тикеру.
        """
        asset_specs = [
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
                'name': 'Тинькофф iMOEX',
                'asset_type': 'ETF',
                'currency': 'RUB',
            },
        ]

        assets: dict[str, Asset] = {}
        for spec in asset_specs:
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
        """Создает демонстрационные транзакции.

        Args:
            account: Брокерский счет.
            assets: Словарь активов.

        Returns:
            int: Количество созданных или обновленных транзакций.
        """
        base_date = now()
        transaction_specs = [
            {
                'external_id': 'demo-123-deposit-1',
                'asset': None,
                'operation_type': 'deposit',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('200000'),
                'date': base_date - timedelta(days=40),
            },
            {
                'external_id': 'demo-123-sber-buy',
                'asset': assets['SBER'],
                'operation_type': 'buy',
                'quantity': Decimal('200'),
                'price_per_unit': Decimal('250'),
                'date': base_date - timedelta(days=35),
            },
            {
                'external_id': 'demo-123-sber-commission',
                'asset': None,
                'operation_type': 'commission',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('300'),
                'date': base_date - timedelta(days=35),
            },
            {
                'external_id': 'demo-123-ofz-buy',
                'asset': assets['OFZ26238'],
                'operation_type': 'buy',
                'quantity': Decimal('60'),
                'price_per_unit': Decimal('1000'),
                'date': base_date - timedelta(days=30),
                'accrued_int': Decimal('150'),
            },
            {
                'external_id': 'demo-123-tmos-buy',
                'asset': assets['TMOS'],
                'operation_type': 'buy',
                'quantity': Decimal('40'),
                'price_per_unit': Decimal('1000'),
                'date': base_date - timedelta(days=25),
            },
            {
                'external_id': 'demo-123-sber-dividend',
                'asset': assets['SBER'],
                'operation_type': 'dividend',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('600'),
                'date': base_date - timedelta(days=20),
            },
            {
                'external_id': 'demo-123-ofz-coupon',
                'asset': assets['OFZ26238'],
                'operation_type': 'coupon',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('1800'),
                'date': base_date - timedelta(days=15),
            },
            {
                'external_id': 'demo-123-sber-sell',
                'asset': assets['SBER'],
                'operation_type': 'sell',
                'quantity': Decimal('50'),
                'price_per_unit': Decimal('280'),
                'date': base_date - timedelta(days=10),
                'yield_amount': Decimal('1500'),
            },
            {
                'external_id': 'demo-123-tax',
                'asset': None,
                'operation_type': 'tax',
                'quantity': Decimal('1'),
                'price_per_unit': Decimal('500'),
                'date': base_date - timedelta(days=7),
            },
            {
                'external_id': 'demo-123-withdrawal',
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

