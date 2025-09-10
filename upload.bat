python setup.py bdist_wheel
twine upload dist/*.whl --config-file %USERPROFILE%\.pypirc  --verbose