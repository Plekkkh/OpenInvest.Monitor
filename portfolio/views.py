from django.views.generic import TemplateView, ListView, CreateView
from django.contrib.auth.mixins import LoginRequiredMixin
from portfolio.models import BrokerAccount, Transaction
from portfolio.services.analytics import AnalyticsService
from portfolio.mixins import OwnerRequiredMixin, CurrentAccountMixin
from django.urls import reverse_lazy
from portfolio.forms import BrokerAccountForm


class DashboardView(LoginRequiredMixin, CurrentAccountMixin, TemplateView):
    template_name = 'portfolio/dashboard.html'

    @staticmethod
    def _get_asset_classes_mapping():
        return {
            'share': 'Акции',
            'bond': 'Облигации',
            'etf': 'Фонды',
            'currency': 'Валюта',
            'crypto': 'Криптовалюта',
        }

    def _initialize_groups(self, asset_classes):
        groups = {}
        for k in asset_classes.values():
            groups[k] = {'name': k, 'current_value': 0.0, 'invested': 0.0, 'yield_amount': 0.0}
        return groups

    def _process_positions(self, positions, groups, asset_classes):
        for pos in positions:
            itype = str(pos.get('instrument_type', '')).lower()
            class_name = asset_classes.get(itype, 'Прочее')
            if class_name not in groups:
                groups[class_name] = {
                    'name': class_name, 'current_value': 0.0, 'invested': 0.0, 'yield_amount': 0.0
                }

            qty = float(pos.get('quantity') or 0)
            price = float(pos.get('current_price') or 0)
            avg = float(pos.get('average_buy_price') or 0)
            yld = float(pos.get('expected_yield') or 0)

            cur_val = qty * price
            inv = qty * avg

            groups[class_name]['current_value'] += cur_val
            groups[class_name]['invested'] += inv
            groups[class_name]['yield_amount'] += yld

    def _process_currencies(self, currencies, groups):
        for cur in currencies:
            class_name = 'Валюта'
            bal = float(cur.get('balance') or 0)
            groups[class_name]['current_value'] += bal
            groups[class_name]['invested'] += bal

    def _calculate_shares_and_yields(self, groups):
        labels = []
        values = []
        portfolio_classes = []
        total_portfolio_calc = sum(g['current_value'] for g in groups.values())

        for g in groups.values():
            cv = g['current_value']
            if cv > 0 or g['invested'] > 0:
                labels.append(g['name'])
                values.append(cv)

                g['share'] = (cv / total_portfolio_calc * 100) if total_portfolio_calc > 0 else 0
                g['yield_percent'] = (g['yield_amount'] / g['invested'] * 100) if g['invested'] > 0 else 0
                portfolio_classes.append(g)

        portfolio_classes = sorted(portfolio_classes, key=lambda x: x['share'], reverse=True)
        return portfolio_classes, labels, values

    @staticmethod
    def _get_fallback_context():
        return {
            'total_value': 0.0,
            'xirr_value': 0.0,
            'profit_metrics': {
                'asset_price_difference': 0.0,
                'realized_pnl': 0.0,
                'accruals': 0.0,
                'taxes': 0.0,
                'commissions': 0.0,
                'aci': 0.0,
                'total_profit': 0.0
            },
            'portfolio_classes': [],
            'labels': [],
            'values': []
        }

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        account = self.get_current_account()

        if not account:
            return context

        try:
            analytics = AnalyticsService(account)
            snapshot = analytics.get_current_portfolio_snapshot()
            total_value = snapshot.get('total_amount', 0.0)
            xirr_value = analytics.calculate_xirr()
            profit_metrics = analytics.get_profit_metrics()

            positions = analytics.get_portfolio_positions()
            asset_classes = self._get_asset_classes_mapping()
            groups = self._initialize_groups(asset_classes)

            self._process_positions(positions, groups, asset_classes)
            self._process_currencies(analytics.get_cash_balance(), groups)

            portfolio_classes, labels, values = self._calculate_shares_and_yields(groups)

        except Exception:
            fallback = self._get_fallback_context()
            total_value = fallback['total_value']
            xirr_value = fallback['xirr_value']
            profit_metrics = fallback['profit_metrics']
            labels = fallback['labels']
            values = fallback['values']
            portfolio_classes = fallback['portfolio_classes']

        context['total_value'] = total_value
        context['xirr_value'] = xirr_value
        context['profit_metrics'] = profit_metrics
        context['chart_data'] = {'labels': labels, 'values': values}
        context['portfolio_classes'] = portfolio_classes

        return context


class TransactionListView(LoginRequiredMixin, OwnerRequiredMixin, CurrentAccountMixin, ListView):
    model = Transaction
    template_name = 'portfolio/transactions.html'
    context_object_name = 'transactions'
    paginate_by = 20

    def get_queryset(self):
        # Base check from OwnerRequiredMixin handles security.
        queryset = super().get_queryset()

        account = self.get_current_account()
        if account:
            return queryset.filter(account=account).select_related('asset').order_by('-date')

        return queryset.none()


class AccountCreateView(LoginRequiredMixin, CreateView):
    model = BrokerAccount
    template_name = 'portfolio/account_form.html'
    form_class = BrokerAccountForm

    def get_success_url(self):
        return reverse_lazy('portfolio:dashboard')

    def form_valid(self, form):
        form.instance.user = self.request.user
        raw_token = form.cleaned_data.get('api_token')
        if raw_token:
            form.instance.api_token = raw_token
        return super().form_valid(form)
