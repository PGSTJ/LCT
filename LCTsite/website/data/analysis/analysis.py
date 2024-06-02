from ...models import BasicAverages, BoxAverages, BoxTracker, CanData, AbbreviationReferences, RawTracker


ALL_FLAVORS = AbbreviationReferences.objects.filter(uid__icontains='FLV').count()
ALL_BOXES = BoxTracker.objects.all().values_list()
print(f'all boxes {ALL_BOXES}')

def flavor_counts():
    """ calculates amount of each flavor """
    return BoxTracker.objects.filter(flavor='PSF').count()
