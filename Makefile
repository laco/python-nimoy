

build:
	python3 setup.py sdist

test:
	tox


clean:
	rm -f dist/*.tar.gz
	rm -rf ve

git_tag:
	git tag $(CURRENT_VERSION)

pypi_upload:
	python3 setup.py sdist upload -r pypi

pypi_register:
	python3 setup.py register -r pypi

pypitest_upload:
	python3 setup.py sdist upload -r pypitest

pypitest_register:
	python3 setup.py register -r pypitest

virtualenv:
	pyvenv ve
	ve/bin/pip install -r requirements.txt
