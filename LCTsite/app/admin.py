from django.contrib import admin
from .models import BasicAverages, BoxAverages, BoxTracker, CanData, AbbreviationReferences, RawTracker


admin.site.register(BasicAverages)
admin.site.register(BoxAverages)
admin.site.register(BoxTracker)
admin.site.register(CanData)
admin.site.register(AbbreviationReferences)
admin.site.register(RawTracker)