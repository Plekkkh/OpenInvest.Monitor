from django.contrib.auth.mixins import AccessMixin
from django.core.exceptions import PermissionDenied
from django.shortcuts import get_object_or_404

class OwnerRequiredMixin(AccessMixin):
    """
    Mixin that verifies that the current user is the owner of the object.
    Requires that self.model has a 'user' field, or for Transactions, follows 'account__user'.
    """
    def dispatch(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            return self.handle_no_permission()

        return super().dispatch(request, *args, **kwargs)

    def get_queryset(self):
        qs = super().get_queryset()
        model = qs.model
        # For BrokerAccount
        if any(f.name == 'user' for f in model._meta.get_fields()):
            return qs.filter(user=self.request.user)
        # For Transaction
        if any(f.name == 'account' for f in model._meta.get_fields()):
            return qs.filter(account__user=self.request.user)
        return qs

class CurrentAccountMixin:
    """
    Миксин для извлечения списка счетов текущего пользователя
    и определения выбранного счета (account_id из GET-параметров).
    """
    def get_user_accounts(self):
        if not hasattr(self, '_user_accounts'):
            from portfolio.models import BrokerAccount
            self._user_accounts = BrokerAccount.objects.filter(user=self.request.user)
        return self._user_accounts

    def get_current_account(self):
        accounts = self.get_user_accounts()
        account_id = self.request.GET.get('account_id')
        if account_id:
            # Получаем конкретный счет, если он принадлежит юзеру
            return accounts.filter(id=account_id).first()
        # Иначе просто первый доступный
        return accounts.first()

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        account = self.get_current_account()
        context['accounts'] = self.get_user_accounts()
        context['account'] = account
        context['current_account'] = account
        return context
