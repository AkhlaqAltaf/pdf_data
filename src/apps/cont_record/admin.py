from django.contrib import admin
from .models import (
    Contract, BuyerDetail, FinancialApproval, PayingAuthority,
    SellerDetail, Product, ProductSpecification, ConsigneeDetail,
    EPBGDetail, TermsAndCondition, OrganisationDetail
)


class ProductSpecificationInline(admin.TabularInline):
    model = ProductSpecification
    extra = 0


class ConsigneeInline(admin.TabularInline):
    model = ConsigneeDetail
    extra = 0


class ProductAdmin(admin.ModelAdmin):
    list_display = ('product_name', 'contract', 'ordered_quantity', 'unit_price', 'total_price')
    search_fields = ('product_name', 'brand', 'model')
    inlines = [ProductSpecificationInline, ConsigneeInline]


class BuyerDetailInline(admin.StackedInline):
    model = BuyerDetail
    extra = 0
    max_num = 1


class FinancialApprovalInline(admin.StackedInline):
    model = FinancialApproval
    extra = 0
    max_num = 1


class PayingAuthorityInline(admin.StackedInline):
    model = PayingAuthority
    extra = 0
    max_num = 1


class SellerDetailInline(admin.StackedInline):
    model = SellerDetail
    extra = 0
    max_num = 1


class EPBGInline(admin.StackedInline):
    model = EPBGDetail
    extra = 0
    max_num = 1


class TermsInline(admin.TabularInline):
    model = TermsAndCondition
    extra = 0

class OrganizationInline(admin.TabularInline):
    model = OrganisationDetail
    extra = 0

class ContractAdmin(admin.ModelAdmin):
    list_display = ('contract_no', 'generated_date')
    search_fields = ('contract_no',)
    inlines = [
        OrganizationInline,
        BuyerDetailInline,
        FinancialApprovalInline,
        PayingAuthorityInline,
        SellerDetailInline,
        EPBGInline,
        TermsInline,
    ]


admin.site.register(Contract, ContractAdmin)
admin.site.register(Product, ProductAdmin)
admin.site.register(TermsAndCondition)
