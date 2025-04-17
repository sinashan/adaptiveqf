# CS7280: Rehashing project

Note that you have to install splinterdb for other tests in the Makefile to work.

## Broom Filter approach

This code lives in the `broom-filter` branch.

Steps to Run:

```bash
git checkout broom-filter
make clean
make -B
./test_qfdb 20 9 $(python3 -c "print(pow(10, 6))") # 1M items
```


Tuning C value:
Update `src/qf_uthash.c:15` with different values and run `make -B`
