from django.views.generic import TemplateView, ListView, CreateView
from django.contrib.auth.mixins import LoginRequiredMixin
import logging
from portfolio.models import BrokerAccount, Transaction
from portfolio.services.analytics import AnalyticsService
from portfolio.mixins import OwnerRequiredMixin, CurrentAccountMixin
from django.urls import reverse_lazy
from portfolio.forms import BrokerAccountForm


logger = logging.getLogger(__name__)


class DashboardView(LoginRequiredMixin, CurrentAccountMixin, TemplateView):
    template_name = 'portfolio/dashboard.html'

    @staticmethod
    def _get_fallback_context() -> dict:
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
            portfolio_classes, labels, values = analytics.get_allocation_data()

        except (ValueError, RuntimeError):
            logger.exception('Ошибка при подготовке данных для dashboard.')
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
