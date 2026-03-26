from django import forms
from django.contrib import admin
from .models import Asset, BrokerAccount, Transaction


@admin.register(Asset)
class AssetAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'name', 'asset_type', 'currency', 'isin')
    search_fields = ('ticker', 'name', 'isin')
    list_filter = ('asset_type', 'currency')


class BrokerAccountForm(forms.ModelForm):
    api_token_input = forms.CharField(
        label="API-токен (сбросит старый, если введен)",
        required=False,
        widget=forms.PasswordInput(render_value=True),
        help_text="Оставьте пустым, чтобы не менять токен."
    )

    class Meta:
        model = BrokerAccount
        fields = ('user', 'name', 'provider_type', 'provider_account_id')

    def save(self, commit=True):
        instance = super().save(commit=False)
        new_token = self.cleaned_data.get('api_token_input')
        if new_token:
            instance.api_token = new_token
        if commit:
            instance.save()
        return instance


@admin.register(BrokerAccount)
class BrokerAccountAdmin(admin.ModelAdmin):
    form = BrokerAccountForm
    list_display = ('name', 'user', 'provider_type', 'provider_account_id')
    search_fields = ('name', 'user__username', 'provider_account_id')
    list_filter = ('provider_type',)


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ('asset', 'account', 'operation_type', 'quantity', 'price_per_unit', 'date', 'total_amount')
    search_fields = ('asset__ticker', 'account__name', 'external_id')
    list_filter = ('operation_type', 'account', 'date')
    date_hierarchy = 'date'
