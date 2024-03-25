from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from .models import Filmwork, Genre, Person, GenreFilmwork, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    autocomplete_fields = ('genre',)


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    autocomplete_fields = ('person',)


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline)
    list_display = ('title', 'type', 'creation_date', 'rating', 'genre_list', 'person_list')
    list_filter = ('type',)
    search_fields = ('title', 'description', 'id')

    list_prefetch_related = ('genres', 'personas')

    def get_queryset(self, request):
        queryset = (
            super()
            .get_queryset(request)
            .prefetch_related(*self.list_prefetch_related)
        )
        return queryset

    @admin.display(description=_('genres'))
    def genre_list(self, obj):
        return ', '.join([genre.name for genre in obj.genres.all()])

    @admin.display(description=_('personas'))
    def person_list(self, obj):
        return ', '.join([person.full_name for person in obj.personas.all()])


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name', 'description',)
    search_fields = ('name',)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name',)
    search_fields = ('full_name',)
