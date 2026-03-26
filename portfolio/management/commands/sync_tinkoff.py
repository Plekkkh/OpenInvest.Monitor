from django.core.management.base import BaseCommand
from portfolio.models import BrokerAccount
from portfolio.services.t_invest import TInvestService


class Command(BaseCommand):
    help = 'Синхронизирует операции с Т-Инвестиций для указанного счета'

    def add_arguments(self, parser):
        parser.add_argument('--account_id', type=int, help='ID брокерского счета в БД', required=False)

    def handle(self, *args, **options):
        account_id = options.get('account_id')

        if account_id:
            accounts = BrokerAccount.objects.filter(id=account_id, provider_type='T-Invest_API')
        else:
            accounts = BrokerAccount.objects.filter(provider_type='T-Invest_API')

        if not accounts.exists():
            self.stdout.write(self.style.WARNING('Не найдено счетов с провайдером T-Invest_API.'))
            return

        for account in accounts:
            self.stdout.write(f'Начинаю синхронизацию счета: {account.name} (user: {account.user.username})...')
            try:
                service = TInvestService(account)
                count = service.sync_operations()
                self.stdout.write(self.style.SUCCESS(f'Успешно! Добавлено новых операций: {count}'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Ошибка при синхронизации счета {account.name}: {str(e)}'))
