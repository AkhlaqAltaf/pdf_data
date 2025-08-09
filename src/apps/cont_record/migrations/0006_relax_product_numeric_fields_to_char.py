from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cont_record', '0005_contract_embedding_product_embedding'),
    ]

    operations = [
        migrations.AlterField(
            model_name='product',
            name='ordered_quantity',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AlterField(
            model_name='product',
            name='unit_price',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AlterField(
            model_name='product',
            name='tax_bifurcation',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
        migrations.AlterField(
            model_name='product',
            name='total_price',
            field=models.CharField(blank=True, max_length=64, null=True),
        ),
    ]


