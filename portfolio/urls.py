from django.urls import path
from .views import DashboardView, TransactionListView, AccountCreateView, DemoPortfolioSeedView

app_name = 'portfolio'

urlpatterns = [
    path('dashboard/', DashboardView.as_view(), name='dashboard'),
    path('transactions/', TransactionListView.as_view(), name='transactions'),
    path('account/add/', AccountCreateView.as_view(), name='add_account'),
    path('demo/seed/', DemoPortfolioSeedView.as_view(), name='seed_demo_portfolio'),
]
