from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

from portfolio.services.demo_portfolio import DemoPortfolioService


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

        user = User.objects.get_or_create(username=username)[0]
        result = DemoPortfolioService().seed_for_user(
            user=user,
            account_name=account_name,
            provider_account_id=provider_account_id,
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"Демо-данные готовы: пользователь '{result.account.user.username}'"
                f"{self._format_created(result.user_created)}, "
                f"счет '{result.account.name}'{self._format_created(result.account_created)}, "
                f"транзакций создано/обновлено: {result.created_transactions}."
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
