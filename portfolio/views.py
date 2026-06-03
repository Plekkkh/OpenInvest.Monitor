from django.views.generic import TemplateView, ListView, CreateView
from django.contrib.auth.mixins import LoginRequiredMixin
import logging
from portfolio.models import BrokerAccount, Transaction
from portfolio.services.analytics import AnalyticsService
from portfolio.mixins import OwnerRequiredMixin, CurrentAccountMixin
from django.urls import reverse_lazy
from portfolio.forms import BrokerAccountForm
from django.http import JsonResponse
from django.template.loader import render_to_string


logger = logging.getLogger(__name__)


class DashboardView(LoginRequiredMixin, CurrentAccountMixin, TemplateView):
    template_name = 'portfolio/dashboard.html'

    @staticmethod
    def _get_fallback_context() -> dict:
        return {
            'total_value': 0.0,
            'xirr_value': 0.0,
            'twr_value': 0.0,
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
            twr_value = analytics.calculate_twr() or 0.0
            profit_metrics = analytics.get_profit_metrics()
            portfolio_classes, labels, values = analytics.get_allocation_data()

        except (ValueError, RuntimeError):
            logger.exception('Ошибка при подготовке данных для dashboard.')
            fallback = self._get_fallback_context()
            total_value = fallback['total_value']
            xirr_value = fallback['xirr_value']
            twr_value = fallback['twr_value']
            profit_metrics = fallback['profit_metrics']
            labels = fallback['labels']
            values = fallback['values']
            portfolio_classes = fallback['portfolio_classes']

        context['total_value'] = total_value
        context['xirr_value'] = xirr_value
        context['twr_value'] = twr_value
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
        account = self.get_current_account()
        if not account:
            return super().get_queryset().none()

        analytics = AnalyticsService(account)
        return analytics.get_transactions_queryset(
            search_query=self.request.GET.get('q', ''),
            operation_type=self.request.GET.get('operation_type', 'all'),
        )

    def get_context_data(self, **kwargs):
        """Добавляет агрегированные суммы по категориям и параметры фильтрации."""
        context = super().get_context_data(**kwargs)

        account = self.get_current_account()
        if account:
            context['category_totals'] = AnalyticsService.get_category_totals(self.object_list)
            context['filtered_count'] = context['paginator'].count if context.get('paginator') else self.object_list.count()
        else:
            context['category_totals'] = {
                'buy': '0.00',
                'sell': '0.00',
                'commission': '0.00',
                'accrual': '0.00',
                'tax': '0.00',
                'deposit': '0.00',
                'withdrawal': '0.00',
            }
            context['filtered_count'] = 0

        query_params = self.request.GET.copy()
        query_params.pop('page', None)
        context['querystring'] = query_params.urlencode()
        context['search_query'] = self.request.GET.get('q', '').strip()
        context['selected_operation_type'] = self.request.GET.get('operation_type', 'all').strip().lower() or 'all'

        return context

    def render_to_response(self, context, **response_kwargs):
        """Возвращает HTML-фрагменты для AJAX-фильтров или полный шаблон.

        Args:
            context: Контекст шаблона.
            **response_kwargs: Дополнительные параметры ответа.

        Returns:
            HttpResponse: JSON с HTML-фрагментами или полный HTML.
        """
        if self.request.headers.get('x-requested-with') == 'XMLHttpRequest' or self.request.GET.get('ajax') == '1':
            return JsonResponse({
                'summary_html': render_to_string('portfolio/partials/transactions_summary.html', context, request=self.request),
                'rows_html': render_to_string('portfolio/partials/transactions_rows.html', context, request=self.request),
                'pagination_html': render_to_string('portfolio/partials/transactions_pagination.html', context, request=self.request),
            })

        return super().render_to_response(context, **response_kwargs)


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
