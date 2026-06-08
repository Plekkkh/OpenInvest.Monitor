from django.test import TestCase
from django.test import RequestFactory
from django.core.management import call_command
from decimal import Decimal
from datetime import timedelta
import json
import pandas as pd
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from django.contrib.auth.models import User
from django.utils.timezone import now
from t_tech.invest import OperationType

from portfolio.models import BrokerAccount, Asset, Transaction
from portfolio.mixins import CurrentAccountMixin
from portfolio.services.analytics import AnalyticsService
from portfolio.services.t_invest import TInvestService
from portfolio.views import TransactionListView


class AnalyticsServiceTests(TestCase):
    """Тесты сервисного слоя аналитики."""

    def _create_account(self, provider_type: str = 'Manual') -> BrokerAccount:
        """Создает тестового пользователя и счет.

        Args:
            provider_type: Тип провайдера счета.

        Returns:
            BrokerAccount: Созданный счет.
        """
        user = User.objects.create_user(username=f'user_{provider_type.lower()}')
        return BrokerAccount.objects.create(
            user=user,
            name=f'Account {provider_type}',
            provider_type=provider_type
        )

    def _create_asset(self, ticker: str = 'AAPL') -> Asset:
        """Создает тестовый актив.

        Args:
            ticker: Тикер инструмента.

        Returns:
            Asset: Созданный актив.
        """
        return Asset.objects.create(
            ticker=ticker,
            isin='US0378331005',
            name='Apple Inc.',
            asset_type='Share',
            currency='USD'
        )

    def _create_transaction(
        self,
        account: BrokerAccount,
        asset: Asset | None,
        operation_type: str,
        quantity: Decimal,
        price_per_unit: Decimal,
        yield_amount: Decimal = Decimal('0'),
        commission_amount: Decimal = Decimal('0'),
        accrued_int: Decimal = Decimal('0')
    ) -> Transaction:
        """Создает тестовую транзакцию.

        Args:
            account: Брокерский счет.
            asset: Актив или None.
            operation_type: Тип операции.
            quantity: Количество.
            price_per_unit: Цена за единицу.
            yield_amount: Доходность.
            commission_amount: Комиссия.
            accrued_int: НКД.

        Returns:
            Transaction: Созданная транзакция.
        """
        return Transaction.objects.create(
            account=account,
            asset=asset,
            operation_type=operation_type,
            quantity=quantity,
            price_per_unit=price_per_unit,
            date=now(),
            yield_amount=yield_amount,
            commission_amount=commission_amount,
            accrued_int=accrued_int
        )

    def test_get_transactions_queryset_search_and_filter(self) -> None:
        """Проверяет поиск и фильтрацию по типу операции."""
        account = self._create_account()
        asset = self._create_asset()
        self._create_transaction(account, asset, 'buy', Decimal('2'), Decimal('100'))
        self._create_transaction(account, asset, 'sell', Decimal('1'), Decimal('150'))

        service = AnalyticsService(account)
        search_qs = service.get_transactions_queryset(search_query='AAPL')
        self.assertEqual(search_qs.count(), 2)

        buy_qs = service.get_transactions_queryset(operation_type='buy')
        self.assertEqual(buy_qs.count(), 1)

    def test_operation_type_filter_deposit_groups(self) -> None:
        """Проверяет объединение категорий пополнений."""
        account = self._create_account()
        self._create_transaction(account, None, 'deposit', Decimal('1'), Decimal('1000'))
        self._create_transaction(account, None, 'other_income', Decimal('1'), Decimal('500'))
        self._create_transaction(account, None, 'other_expense', Decimal('1'), Decimal('200'))

        service = AnalyticsService(account)
        deposit_qs = service.get_transactions_queryset(operation_type='deposit')
        self.assertEqual(deposit_qs.count(), 2)

    @patch('portfolio.services.analytics.pyxirr.xirr', return_value=0.1)
    def test_calculate_xirr_returns_percent(self, mocked_xirr: Any) -> None:
        """Проверяет расчет XIRR и конвертацию в проценты."""
        account = self._create_account()
        self._create_transaction(account, None, 'deposit', Decimal('1'), Decimal('1000'))
        self._create_transaction(account, None, 'withdrawal', Decimal('1'), Decimal('1200'))

        service = AnalyticsService(account)
        with patch.object(service, 'get_current_portfolio_snapshot', return_value={'total_amount': Decimal('0')}):
            result = service.calculate_xirr()

        self.assertEqual(result, 10.0)
        mocked_xirr.assert_called_once()

    def test_get_current_portfolio_snapshot_uses_api(self) -> None:
        """Проверяет использование API-сервиса для снимка портфеля."""
        account = self._create_account(provider_type='T-Invest_API')

        class FakeTInvestService:
            def __init__(self, _account: BrokerAccount) -> None:
                pass

            def get_portfolio(self) -> dict[str, Any]:
                return {'total_amount': Decimal('1000'), 'positions': [], 'currencies': []}

        with patch('portfolio.services.analytics.TInvestService', FakeTInvestService):
            service = AnalyticsService(account)
            snapshot = service.get_current_portfolio_snapshot()

        self.assertEqual(snapshot['total_amount'], Decimal('1000'))

    def test_get_allocation_data_from_positions_and_cash(self) -> None:
        """Проверяет расчет распределения по классам активов."""
        account = self._create_account()
        service = AnalyticsService(account)

        positions = [
            {
                'instrument_type': 'share',
                'quantity': Decimal('2'),
                'current_price': Decimal('100'),
                'average_buy_price': Decimal('80'),
                'expected_yield': Decimal('40')
            }
        ]
        currencies = [{'balance': Decimal('500')}]

        with patch.object(service, 'get_portfolio_positions', return_value=positions), \
                patch.object(service, 'get_cash_balance', return_value=currencies):
            portfolio_classes, labels, values = service.get_allocation_data()

        self.assertIn('Акции', labels)
        self.assertIn('Валюта', labels)
        self.assertEqual(sum(values), 700.0)
        self.assertTrue(any(item['name'] == 'Акции' for item in portfolio_classes))

    def test_get_profit_metrics_aggregates(self) -> None:
        """Проверяет агрегирование метрик прибыли."""
        account = self._create_account()
        asset = self._create_asset()

        self._create_transaction(account, asset, 'sell', Decimal('1'), Decimal('100'), yield_amount=Decimal('20'))
        self._create_transaction(account, asset, 'coupon', Decimal('1'), Decimal('10'))
        self._create_transaction(account, asset, 'tax', Decimal('1'), Decimal('3'))
        self._create_transaction(account, asset, 'commission', Decimal('1'), Decimal('2'))
        self._create_transaction(account, asset, 'buy', Decimal('1'), Decimal('50'), accrued_int=Decimal('1'))

        service = AnalyticsService(account)
        positions = [{'expected_yield': Decimal('5'), 'current_nkd': Decimal('1')}]

        with patch.object(service, 'get_portfolio_positions', return_value=positions):
            metrics = service.get_profit_metrics()

        self.assertEqual(metrics['asset_price_difference'], Decimal('5'))
        self.assertEqual(metrics['realized_pnl'], Decimal('20'))
        self.assertEqual(metrics['accruals'], Decimal('10'))
        self.assertEqual(metrics['taxes'], Decimal('3'))
        self.assertEqual(metrics['commissions'], Decimal('2'))
        self.assertEqual(metrics['aci'], Decimal('0'))
        self.assertEqual(metrics['total_profit'], Decimal('30'))

    def test_calculate_twr_handles_price_growth(self) -> None:
        """Проверяет TWR при росте цены без промежуточных внешних потоков."""
        account = self._create_account()
        asset = self._create_asset('SBER')
        base_date = now() - timedelta(days=30)

        Transaction.objects.create(
            account=account,
            operation_type='deposit',
            quantity=Decimal('1'),
            price_per_unit=Decimal('1000'),
            date=base_date,
        )
        Transaction.objects.create(
            account=account,
            asset=asset,
            operation_type='buy',
            quantity=Decimal('10'),
            price_per_unit=Decimal('100'),
            date=base_date,
        )

        class FakePriceProvider:
            def get_price_matrix(self, account: BrokerAccount, asset_ids: list[int], valuation_dates: pd.DatetimeIndex) -> pd.DataFrame:
                matrix = pd.DataFrame(Decimal('0'), index=valuation_dates, columns=asset_ids)
                matrix.loc[valuation_dates[0], asset_ids[0]] = Decimal('100')
                matrix.loc[valuation_dates[-1], asset_ids[0]] = Decimal('110')
                return matrix.ffill()

        service = AnalyticsService(account, price_provider=FakePriceProvider())
        twr = service.calculate_twr()

        self.assertAlmostEqual(twr or 0.0, 10.0, places=6)

    def test_calculate_twr_ignores_mid_period_deposit(self) -> None:
        """Проверяет, что пополнение в середине периода не искажает TWR."""
        account = self._create_account()
        asset = self._create_asset('GAZP')
        start_date = now() - timedelta(days=20)
        middle_date = now() - timedelta(days=10)

        Transaction.objects.create(
            account=account,
            operation_type='deposit',
            quantity=Decimal('1'),
            price_per_unit=Decimal('1000'),
            date=start_date,
        )
        Transaction.objects.create(
            account=account,
            asset=asset,
            operation_type='buy',
            quantity=Decimal('10'),
            price_per_unit=Decimal('100'),
            date=start_date,
        )
        Transaction.objects.create(
            account=account,
            operation_type='deposit',
            quantity=Decimal('1'),
            price_per_unit=Decimal('500'),
            date=middle_date,
        )

        class FakePriceProvider:
            def get_price_matrix(self, account: BrokerAccount, asset_ids: list[int], valuation_dates: pd.DatetimeIndex) -> pd.DataFrame:
                matrix = pd.DataFrame(Decimal('0'), index=valuation_dates, columns=asset_ids)
                matrix.loc[valuation_dates[0], asset_ids[0]] = Decimal('100')
                matrix.loc[valuation_dates[1], asset_ids[0]] = Decimal('120')
                matrix.loc[valuation_dates[-1], asset_ids[0]] = Decimal('120')
                return matrix.ffill()

        service = AnalyticsService(account, price_provider=FakePriceProvider())
        twr = service.calculate_twr()

        self.assertAlmostEqual(twr or 0.0, 20.0, places=6)

    def test_calculate_twr_matches_reference_example(self) -> None:
        """Проверяет TWR по эталонному примеру с пополнением в середине периода."""
        account = self._create_account()
        asset = self._create_asset('LKOH')
        start_date = now() - timedelta(days=20)
        middle_date = now() - timedelta(days=10)

        Transaction.objects.create(
            account=account,
            operation_type='deposit',
            quantity=Decimal('1'),
            price_per_unit=Decimal('10000'),
            date=start_date,
        )
        Transaction.objects.create(
            account=account,
            asset=asset,
            operation_type='buy',
            quantity=Decimal('100'),
            price_per_unit=Decimal('100'),
            date=start_date,
        )
        Transaction.objects.create(
            account=account,
            operation_type='deposit',
            quantity=Decimal('1'),
            price_per_unit=Decimal('5000'),
            date=middle_date,
        )

        class FakePriceProvider:
            def get_price_matrix(self, account: BrokerAccount, asset_ids: list[int], valuation_dates: pd.DatetimeIndex) -> pd.DataFrame:
                matrix = pd.DataFrame(Decimal('0'), index=valuation_dates, columns=asset_ids)
                matrix.loc[valuation_dates[0], asset_ids[0]] = Decimal('100')
                matrix.loc[valuation_dates[1], asset_ids[0]] = Decimal('105')
                matrix.loc[valuation_dates[-1], asset_ids[0]] = Decimal('110')
                return matrix.ffill()

        service = AnalyticsService(account, price_provider=FakePriceProvider())
        twr = service.calculate_twr()

        # (1 + 0.05) * (1 + 500/15500) - 1 = 0.0838709677...
        self.assertAlmostEqual(twr or 0.0, 8.38709677, places=6)


class TInvestServiceTests(TestCase):
    """Тесты сервиса синхронизации с T-Invest."""

    def _create_account(self) -> BrokerAccount:
        """Создает счет с токеном и ID провайдера."""
        user = User.objects.create_user(username='tinvest_user')
        account = BrokerAccount.objects.create(
            user=user,
            name='T-Invest account',
            provider_type='T-Invest_API',
            provider_account_id='acc-1'
        )
        account.api_token = 'token-123'
        account.save()
        return account

    @staticmethod
    def _make_operation(
        op_id: str,
        op_type: Any,
        payment: Decimal,
        price: Decimal,
        quantity: Decimal,
        figi: str = 'FIGI-1',
        instrument_uid: str = 'UID-1',
        parent_operation_id: str | None = None,
    ) -> SimpleNamespace:
        """Создает мок операции T-Invest."""
        payment_box = SimpleNamespace(value=payment)
        price_box = SimpleNamespace(value=price)
        yield_box = SimpleNamespace(value=Decimal('0'))
        commission_box = SimpleNamespace(value=Decimal('0'))
        accrued_box = SimpleNamespace(value=Decimal('0'))

        return SimpleNamespace(
            id=op_id,
            type=op_type,
            payment=payment_box,
            price=price_box,
            quantity=quantity,
            figi=figi,
            instrument_uid=instrument_uid,
            date=now(),
            parent_operation_id=parent_operation_id,
            yield_=yield_box,
            commission=commission_box,
            accrued_int=accrued_box,
        )

    def test_sync_operations_creates_transactions_and_restores_parent_links(self) -> None:
        """Проверяет создание транзакций, дедупликацию и восстановление связей."""
        account = self._create_account()
        asset_data = {
            'instrument_uid': 'UID-1',
            'figi': 'FIGI-1',
            'ticker': 'AAPL',
            'isin': 'US0378331005',
            'name': 'Apple Inc.',
            'instrument_type': 'share',
            'currency': 'USD',
        }

        existing_asset = Asset.objects.create(
            instrument_uid='UID-1',
            figi='FIGI-1',
            ticker='AAPL',
            isin='US0378331005',
            name='Apple Inc.',
            asset_type='Share',
            currency='USD',
        )
        Transaction.objects.create(
            account=account,
            external_id='op-existing',
            asset=existing_asset,
            operation_type='sell',
            quantity=Decimal('1'),
            price_per_unit=Decimal('100'),
            date=now(),
        )

        parent_op = self._make_operation(
            op_id='op-parent',
            op_type=OperationType.OPERATION_TYPE_BUY,
            payment=Decimal('-200'),
            price=Decimal('100'),
            quantity=Decimal('2'),
        )
        child_op = self._make_operation(
            op_id='op-child',
            op_type=OperationType.OPERATION_TYPE_BROKER_FEE,
            payment=Decimal('-5'),
            price=Decimal('5'),
            quantity=Decimal('1'),
            parent_operation_id='op-parent',
        )
        duplicate_op = self._make_operation(
            op_id='op-existing',
            op_type=OperationType.OPERATION_TYPE_SELL,
            payment=Decimal('210'),
            price=Decimal('105'),
            quantity=Decimal('2'),
        )
        fake_response = SimpleNamespace(items=[parent_op, child_op, duplicate_op], has_next=False, next_cursor=None)
        fake_client = SimpleNamespace(
            operations=SimpleNamespace(get_operations_by_cursor=SimpleNamespace(return_value=None))
        )

        def fake_get_operations_by_cursor(request: Any) -> SimpleNamespace:
            return fake_response

        fake_client.operations.get_operations_by_cursor = fake_get_operations_by_cursor

        class FakeRetryingClient:
            def __init__(self, token: str, settings: Any) -> None:
                self.token = token
                self.settings = settings

            def __enter__(self) -> SimpleNamespace:
                return fake_client

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        with patch('portfolio.services.t_invest.RetryingClient', FakeRetryingClient), \
                patch('portfolio.services.t_invest.quotation_to_decimal', side_effect=lambda value: getattr(value, 'value', Decimal('0'))), \
                patch.object(TInvestService, '_get_or_build_instruments_index', return_value={'FIGI-1': asset_data}):
            service = TInvestService(account)
            saved_count = service.sync_operations(
                from_date=now() - timedelta(days=1),
                to_date=now() + timedelta(days=1),
            )

        self.assertEqual(saved_count, 2)
        self.assertEqual(Transaction.objects.filter(account=account).count(), 3)

        child_tx = Transaction.objects.get(external_id='op-child')
        parent_tx = Transaction.objects.get(external_id='op-parent')
        self.assertEqual(getattr(child_tx, 'parent_transaction_id'), getattr(parent_tx, 'pk'))
        self.assertEqual(child_tx.asset.ticker, 'AAPL')

    def test_sync_operations_skips_duplicate_items(self) -> None:
        """Проверяет, что дубликаты операций не создаются повторно."""
        account = self._create_account()
        Transaction.objects.create(
            account=account,
            external_id='op-existing',
            operation_type='sell',
            quantity=Decimal('1'),
            price_per_unit=Decimal('100'),
            date=now(),
        )

        duplicate_op = self._make_operation(
            op_id='op-existing',
            op_type=OperationType.OPERATION_TYPE_SELL,
            payment=Decimal('210'),
            price=Decimal('105'),
            quantity=Decimal('2'),
        )
        fake_response = SimpleNamespace(items=[duplicate_op], has_next=False, next_cursor=None)

        class FakeRetryingClient:
            def __init__(self, token: str, settings: Any) -> None:
                self.token = token
                self.settings = settings

            def __enter__(self) -> SimpleNamespace:
                return SimpleNamespace(
                    operations=SimpleNamespace(get_operations_by_cursor=lambda request: fake_response)
                )

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

        with patch('portfolio.services.t_invest.RetryingClient', FakeRetryingClient), \
                patch('portfolio.services.t_invest.quotation_to_decimal', side_effect=lambda value: getattr(value, 'value', Decimal('0'))), \
                patch.object(TInvestService, '_get_or_build_instruments_index', return_value={}):
            service = TInvestService(account)
            saved_count = service.sync_operations(
                from_date=now() - timedelta(days=1),
                to_date=now() + timedelta(days=1),
            )

        self.assertEqual(saved_count, 0)
        self.assertEqual(Transaction.objects.filter(account=account).count(), 1)


class CurrentAccountMixinTests(TestCase):
    """Проверяет защиту выбора текущего счета пользователя."""

    def test_get_current_account_rejects_foreign_account_id(self) -> None:
        """Чужой счет не должен подхватываться по account_id."""
        user = User.objects.create_user(username='owner')
        foreign_user = User.objects.create_user(username='foreign')
        own_account = BrokerAccount.objects.create(
            user=user,
            name='Own account',
            provider_type='Manual'
        )
        foreign_account = BrokerAccount.objects.create(
            user=foreign_user,
            name='Foreign account',
            provider_type='Manual'
        )

        request = RequestFactory().get('/', {'account_id': foreign_account.pk})
        request.user = user

        class DummyView(CurrentAccountMixin):
            def __init__(self, req: Any) -> None:
                self.request = req

        view = DummyView(request)
        self.assertIsNone(view.get_current_account())

    def test_get_current_account_returns_own_account(self) -> None:
        """Свой счет выбирается корректно по account_id."""
        user = User.objects.create_user(username='owner_own')
        account = BrokerAccount.objects.create(
            user=user,
            name='Own account',
            provider_type='Manual'
        )

        request = RequestFactory().get('/', {'account_id': account.pk})
        request.user = user

        class DummyView(CurrentAccountMixin):
            def __init__(self, req: Any) -> None:
                self.request = req

        view = DummyView(request)
        self.assertEqual(view.get_current_account(), account)


class TransactionAjaxViewTests(TestCase):
    """Проверяет AJAX-ответ списка операций."""

    def test_transactions_view_returns_json_for_ajax_request(self) -> None:
        """AJAX-запрос должен возвращать HTML-фрагменты в JSON."""
        user = User.objects.create_user(username='ajax_user')
        account = BrokerAccount.objects.create(
            user=user,
            name='Ajax account',
            provider_type='Manual'
        )
        asset = Asset.objects.create(
            ticker='AAPL',
            isin='US0378331005',
            name='Apple Inc.',
            asset_type='Share',
            currency='USD'
        )
        Transaction.objects.create(
            account=account,
            asset=asset,
            operation_type='buy',
            quantity=Decimal('1'),
            price_per_unit=Decimal('100'),
            date=now(),
        )

        request = RequestFactory().get('/', {'account_id': account.pk, 'ajax': '1'}, HTTP_X_REQUESTED_WITH='XMLHttpRequest')
        request.user = user

        response = TransactionListView.as_view()(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get('Content-Type'), 'application/json')
        payload = json.loads(getattr(response, 'content').decode('utf-8'))
        self.assertIn('summary_html', payload)
        self.assertIn('rows_html', payload)
        self.assertIn('pagination_html', payload)


class DemoPortfolioSeedCommandTests(TestCase):
    """Проверяет создание демонстрационных данных для дашборда."""

    def test_seed_demo_portfolio_creates_dashboard_data(self) -> None:
        """Команда сидирования должна создавать заполненный демо-портфель."""
        call_command('seed_demo_portfolio', verbosity=0)

        account = BrokerAccount.objects.get(user__username='123', name='Test_Inv')
        service = AnalyticsService(account)
        snapshot = service.get_current_portfolio_snapshot()
        labels = service.get_allocation_data()[1]

        self.assertEqual(account.provider_type, 'Manual')
        self.assertGreater(Asset.objects.count(), 0)
        self.assertGreater(Transaction.objects.filter(account=account).count(), 0)
        self.assertGreater(snapshot['total_amount'], Decimal('0'))
        self.assertTrue(snapshot['positions'])
        self.assertTrue(snapshot['currencies'])
        self.assertIn('Акции', labels)
        self.assertIn('Облигации', labels)
        self.assertIn('Валюта', labels)


class DemoPortfolioSeedViewTests(TestCase):
    """Проверяет создание демо-портфеля через веб-кнопку."""

    def test_seed_demo_portfolio_view_creates_data_and_redirects(self) -> None:
        """POST-запрос должен создать демо-портфель и вернуть редирект."""
        user = User.objects.create_user(username='teacher')
        self.client.force_login(user)

        response = self.client.post('/portfolio/demo/seed/')

        self.assertEqual(response.status_code, 302)
        self.assertIn('/portfolio/dashboard/', response.url)

        account = BrokerAccount.objects.get(user=user, name='Test_Inv')
        service = AnalyticsService(account)
        snapshot = service.get_current_portfolio_snapshot()

        self.assertGreater(Transaction.objects.filter(account=account).count(), 0)
        self.assertGreater(snapshot['total_amount'], Decimal('0'))
        self.assertContains(
            self.client.get(response.url),
            'Демо-портфель готов',
            status_code=200,
        )


