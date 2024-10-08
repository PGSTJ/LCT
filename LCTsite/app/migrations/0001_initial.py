# Generated by Django 5.1.1 on 2024-09-29 23:53

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='AbbreviationReferences',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('uid', models.CharField(max_length=10)),
                ('abbreviation', models.CharField(max_length=3)),
                ('name', models.CharField(max_length=40)),
            ],
        ),
        migrations.CreateModel(
            name='BasicAverages',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('uid', models.CharField(max_length=10)),
                ('name', models.CharField(max_length=45)),
                ('value', models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name='BoxTracker',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('bid', models.CharField(max_length=20)),
                ('flavor', models.CharField(max_length=25)),
                ('purchase_date', models.CharField(max_length=15)),
                ('price', models.CharField(max_length=10)),
                ('location', models.CharField(max_length=5)),
                ('started', models.CharField(max_length=15)),
                ('finished', models.CharField(max_length=15)),
                ('contributing', models.BooleanField()),
                ('filled', models.BooleanField()),
            ],
        ),
        migrations.CreateModel(
            name='RawTracker',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('value', models.CharField(max_length=10)),
                ('amount', models.IntegerField()),
            ],
        ),
        migrations.CreateModel(
            name='BoxAverages',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('initial_grams', models.IntegerField()),
                ('initial_floz', models.IntegerField()),
                ('final_grams', models.IntegerField()),
                ('final_floz', models.IntegerField()),
                ('percent_remaining_g', models.IntegerField()),
                ('percent_remaining_floz', models.IntegerField()),
                ('bid', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='app.boxtracker')),
            ],
        ),
        migrations.CreateModel(
            name='CanData',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('cid', models.CharField(max_length=40)),
                ('initial_grams', models.CharField(max_length=3)),
                ('initial_floz', models.CharField(max_length=4)),
                ('final_grams', models.CharField(max_length=3)),
                ('final_floz', models.CharField(max_length=4)),
                ('finished', models.CharField(max_length=2)),
                ('percent_remaining_g', models.CharField(max_length=5)),
                ('percent_remaining_floz', models.CharField(max_length=5)),
                ('bid', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='app.boxtracker')),
            ],
        ),
    ]
