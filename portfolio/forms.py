from django import forms
from portfolio.models import BrokerAccount


class BrokerAccountForm(forms.ModelForm):
    api_token = forms.CharField(
        widget=forms.PasswordInput(attrs={'placeholder': 'API Токен (t.v2...abcd)'}),
        required=False,
        label="API-токен (опционально, будет зашифрован)"
    )

    class Meta:
        model = BrokerAccount
        fields = ['name', 'provider_type', 'provider_account_id']
        labels = {
            'name': 'Название счета',
            'provider_type': 'Провайдер',
            'provider_account_id': 'ID счета у провайдера',
        }
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-input', 'placeholder': 'Мой портфель'}),
            'provider_type': forms.Select(attrs={'class': 'form-input'}),
            'provider_account_id': forms.TextInput(attrs={'class': 'form-input'}),
        }
