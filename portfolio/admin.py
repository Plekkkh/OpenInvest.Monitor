from django import forms
from django.contrib import admin
from .models import Asset, BrokerAccount, Transaction


class BrokerAccountAdminForm(forms.ModelForm):
    # Создаем виртуальное поле для ввода токена
    api_token = forms.CharField(
        widget=forms.PasswordInput(render_value=True),  # Чтобы токен выглядел как точки
        required=False,
        label="API-токен (введите для обновления)",
        help_text="Токен будет зашифрован перед сохранением."
    )

    class Meta:
        model = BrokerAccount
        fields = '__all__'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Если мы редактируем существующий объект, подтягиваем расшифрованный токен в поле
        if self.instance and self.instance.pk:
            self.fields['api_token'].initial = self.instance.api_token

    def save(self, commit=True):
        # При сохранении формы передаем значение из поля ввода в наш @api_token.setter
        instance = super().save(commit=False)
        instance.api_token = self.cleaned_data.get('api_token')
        if commit:
            instance.save()
        return instance


@admin.register(Asset)
class AssetAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'name', 'asset_type', 'currency')
    search_fields = ('ticker', 'name', 'isin')
    list_filter = ('asset_type', 'currency')


@admin.register(BrokerAccount)
class BrokerAccountAdmin(admin.ModelAdmin):
    form = BrokerAccountAdminForm
    list_display = ('name', 'user', 'provider_type')
    list_filter = ('provider_type',)
    search_fields = ('name', 'user__username')
    exclude = ('_encrypted_token',)  # Мы не хотим показывать зашифрованный токен в админке


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ('operation_type', 'account', 'asset', 'quantity', 'price_per_unit', 'date')
    list_filter = ('operation_type', 'account', 'date')
    search_fields = ('account__name', 'asset__ticker')
    date_hierarchy = 'date'
