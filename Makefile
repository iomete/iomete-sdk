.PHONY: clean build check release

clean:
	rm -rf build dist *.egg-info

build: clean
	pip install --upgrade build twine
	python -m build

check: build
	twine check dist/*

release: check
	twine upload dist/*