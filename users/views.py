from django.shortcuts import render, redirect
from django.contrib.auth.views import LoginView as BaseLoginView, LogoutView as BaseLogoutView
from django.views.generic import FormView
from django.urls import reverse_lazy
from django.contrib.auth import login
from django.conf import settings
from .forms import RegistrationForm
from .services import UserService

class LoginView(BaseLoginView):
    template_name = 'users/login.html'
    redirect_authenticated_user = True

class LogoutView(BaseLogoutView):
    pass

class RegisterView(FormView):
    template_name = 'users/register.html'
    form_class = RegistrationForm

    def get_success_url(self):
        return settings.LOGIN_REDIRECT_URL

    def form_valid(self, form):
        username = form.cleaned_data['username']
        email = form.cleaned_data['email']
        password = form.cleaned_data['password']

        user = UserService.create_user(username, email, password)
        login(self.request, user)
        return redirect(self.get_success_url())

    def dispatch(self, request, *args, **kwargs):
        if request.user.is_authenticated:
            return redirect(settings.LOGIN_REDIRECT_URL)
        return super().dispatch(request, *args, **kwargs)
