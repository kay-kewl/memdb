= 1. В папку memdb_project надо добавить директорию external, в который надо скачать googletest.

```
memdb_project/external/googletest-1.15.2$ ls
BUILD.bazel      CONTRIBUTORS          googletest           README.md
ci               docs                  googletest_deps.bzl  WORKSPACE
CMakeLists.txt   fake_fuchsia_sdk.bzl  LICENSE              WORKSPACE.bzlmod
CONTRIBUTING.md  googlemock            MODULE.bazel
```

= 2. Запустить тесты
```
mkdir build
cd build
cmake ..
make
./run_tests
```
