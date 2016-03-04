# -*- coding: utf-8 -*-

from setup import extensions, runsetup, Extension, setup_requires

extensions[:] = []

extensions.append(
    Extension(
        name="mbufferio._mbufferio",
        sources=['mbufferio/_mbufferio.pyx', 'mbufferio/murmur.c'],
        language="c"
    )
)

setup_requires.append('cython')

if __name__ == "__main__":
    runsetup()

