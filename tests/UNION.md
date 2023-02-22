# using union data type

## pyarrow

Pyarrow can not handle union data type at the moment. For a properly built
union it says:

        pyarrow.lib.ArrowNotImplementedError: Unknown or unsupported Arrow type: dense_union<int64: int64=0, float64: double=1, string: string=2>

See union1.py that was sent to github when reporting the issue https://github.com/apache/arrow/issues/34262.

See also union0.py used for the initial test.


## pyorc

A simplistic non-pyarrow ORC file writer python module pyorc is able to write unions with some minor
constraints.

See https://github.com/noirello/pyorc/issues/61.

See union2.py.
