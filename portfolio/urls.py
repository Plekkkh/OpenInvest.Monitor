from django.urls import path
from .views import DashboardView, TransactionListView

app_name = 'portfolio'

urlpatterns = [
    path('dashboard/', DashboardView.as_view(), name='dashboard'),
    path('transactions/', TransactionListView.as_view(), name='transactions'),
]

