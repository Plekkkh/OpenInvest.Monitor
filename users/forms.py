from django import forms
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError
import re


COMMON_INPUT_ATTRS = {
    'class': 'form-input',
    'style': 'width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 8px;',
}


class RegistrationForm(forms.ModelForm):
    password = forms.CharField(widget=forms.PasswordInput(attrs={**COMMON_INPUT_ATTRS, 'placeholder': 'Пароль'}))
    password_confirm = forms.CharField(widget=forms.PasswordInput(attrs={**COMMON_INPUT_ATTRS, 'placeholder': 'Подтвердите пароль'}))

    class Meta:
        model = User
        fields = ['username', 'email']
        widgets = {
            'username': forms.TextInput(attrs={**COMMON_INPUT_ATTRS, 'placeholder': 'Имя пользователя'}),
            'email': forms.EmailInput(attrs={**COMMON_INPUT_ATTRS, 'placeholder': 'Email'}),
        }

    def clean_password(self):
        password = self.cleaned_data.get('password')
        if not password:
            return password
        password = str(password)
        if len(password) < 8:
            raise ValidationError("Пароль должен содержать минимум 8 символов.")
        if not re.search(r'\d', password):
            raise ValidationError("Пароль должен содержать хотя бы одну цифру.")
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            raise ValidationError("Пароль должен содержать хотя бы один спецсимвол.")
        return password

    def clean(self):
        cleaned_data = super().clean()
        password = cleaned_data.get("password")
        password_confirm = cleaned_data.get("password_confirm")

        if password and password_confirm and password != password_confirm:
            raise ValidationError("Пароли не совпадают.")
        return cleaned_data

