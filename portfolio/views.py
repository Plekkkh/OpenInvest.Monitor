import json
from django.shortcuts import render, redirect
from django.views.generic import TemplateView, ListView, CreateView
from django.contrib.auth.mixins import LoginRequiredMixin
from portfolio.models import BrokerAccount, Transaction
from portfolio.services.analytics import AnalyticsService
from portfolio.mixins import OwnerRequiredMixin, CurrentAccountMixin
from django.urls import reverse_lazy
from django.conf import settings
from portfolio.forms import BrokerAccountForm

class DashboardView(LoginRequiredMixin, CurrentAccountMixin, TemplateView):
    template_name = 'portfolio/dashboard.html'

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

        except Exception as e:
            # Fallback if API or provider crashes
            total_value = 0.0
            xirr_value = 0.0
            labels = []
            values = []

        chart_data = {
            'labels': labels,
            'values': values,
        }

        context['total_value'] = total_value
        context['xirr_value'] = xirr_value
        context['chart_data'] = chart_data

        # Optimize query with select_related for asset
        context['recent_transactions'] = Transaction.objects.filter(
            account=account
        ).select_related('asset').order_by('-date')[:5]

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
