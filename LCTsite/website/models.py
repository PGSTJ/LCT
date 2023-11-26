from django.db import models

# Create your models here.

class AbbreviationReferences(models.Model):
    uid = models.CharField(max_length=10)
    abbreviation = models.CharField(max_length=3)
    name = models.CharField(max_length=40)


class BoxTracker(models.Model):
    bid = models.CharField(max_length=20)
    flavor = models.CharField(max_length=3)
    purchase_date = models.CharField(max_length=15)
    price = models.CharField(max_length=10)
    location = models.CharField(max_length=5)
    started = models.CharField(max_length=15)
    finished = models.CharField(max_length=15)
    contributing = models.BooleanField()
    filled = models.BooleanField()

    def data(self) -> dict:
        return {
                'id': self.bid,
                'flavor': self.flavor,
                'purchase date': self.purchase_date,
                'price': self.price,
                'purchase location': self.location,
                'start date': self.started,
                'finish date': self.finished,
            }
    
    def is_filled(self) -> bool:
        return self.filled
    
    def is_contributing(self) -> bool:
        return self.contributing


class BoxAverages(models.Model):
    bid = models.ForeignKey(BoxTracker, on_delete=models.CASCADE)
    initial_grams = models.IntegerField()
    initial_floz = models.IntegerField()
    final_grams = models.IntegerField()
    final_floz = models.IntegerField()
    percent_remaining_g = models.IntegerField()
    percent_remaining_floz = models.IntegerField()

    def stats(self) -> dict:
        return {
            self.bid: [
                self.initial_grams,
                self.initial_floz,
                self.final_grams,
                self.final_floz,
                self.percent_remaining_g,
                self.percent_remaining_floz
            ]
        }

class CanData(models.Model):
    cid = models.CharField(max_length=40)
    bid = models.ForeignKey(BoxTracker, on_delete=models.CASCADE)
    initial_grams = models.CharField(max_length=3)
    initial_floz = models.CharField(max_length=4)
    final_grams = models.CharField(max_length=3)
    final_floz = models.CharField(max_length=4)
    finished = models.CharField(max_length=2)
    percent_remaining_g = models.CharField(max_length=5)
    percent_remaining_floz = models.CharField(max_length=5)


    def stats(self) -> dict:
        return {
            'cid': self.cid,
            'bid': self.bid,
            'inital weight': self.initial_grams,
            'inital volume': self.initial_floz,
            'final weight': self.final_grams,
            'final volume': self.final_floz,
            'percent weight loss': self.percent_remaining_g,
            'percent volume loss': self.percent_remaining_floz,
        }
    
    def is_finished(self) -> bool:
        return self.finished


class BasicAverages(models.Model):
    uid = models.CharField(max_length=10)
    name = models.CharField(max_length=45)
    value = models.IntegerField()
