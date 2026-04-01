import json
from django.shortcuts import render
from django.views.generic import TemplateView, ListView
from portfolio.models import BrokerAccount, Transaction
from portfolio.services.analytics import AnalyticsService

class DashboardView(TemplateView):
    template_name = 'portfolio/dashboard.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Determine the target account (optional multi-account scope)
        account_id = self.request.GET.get('account_id')
        if account_id:
            account = BrokerAccount.objects.filter(id=account_id).first()
        else:
            account = BrokerAccount.objects.first()

        if not account:
            return context

        analytics = AnalyticsService(account)
        snapshot = analytics.get_current_portfolio_snapshot()
        total_value = snapshot.get('total_amount', 0.0)
        xirr_value = analytics.calculate_xirr()

        # Prepare chart data
        positions = analytics.get_portfolio_positions()

        # Transform positions to JSON for Plotly
        labels = []
        values = []
        for pos in positions:
            labels.append(pos.get('ticker') or pos.get('figi') or 'Unknown')
            values.append(float(pos.get('current_price') or 0) * float(pos.get('quantity') or 0))

        currencies = analytics.get_cash_balance()
        for cur in currencies:
            labels.append((cur.get('currency') or 'RUB').upper())
            values.append(float(cur.get('balance') or 0))

        chart_data = {
            'labels': labels,
            'values': values,
        }

        context['account'] = account
        context['total_value'] = total_value
        context['xirr_value'] = xirr_value
        context['chart_data'] = chart_data
        context['recent_transactions'] = Transaction.objects.filter(account=account).order_by('-date')[:5]
        context['accounts'] = BrokerAccount.objects.all()

        return context

class TransactionListView(ListView):
    model = Transaction
    template_name = 'portfolio/transactions.html'
    context_object_name = 'transactions'
    paginate_by = 20

    def get_queryset(self):
        queryset = super().get_queryset()
        account_id = self.request.GET.get('account_id')
        if account_id:
            queryset = queryset.filter(account_id=account_id)
        else:
            account = BrokerAccount.objects.first()
            if account:
                queryset = queryset.filter(account=account)
            else:
                queryset = queryset.none()

        return queryset.select_related('asset').order_by('-date')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        account_id = self.request.GET.get('account_id')
        if account_id:
            context['current_account'] = BrokerAccount.objects.filter(id=account_id).first()
        else:
            context['current_account'] = BrokerAccount.objects.first()
        context['accounts'] = BrokerAccount.objects.all()
        return context
