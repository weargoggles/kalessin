# Kalessin is:

- an experiment in Python metaclasses
- an homage to Codd's relational algebra of sets
- a wrapper around @tomchristie's excellent [databases](//github.com/encode/databases)

## e.g.

```python
import databases
import kalessin.sets

class Note(kalessin.sets.Set):
  id: int
  text: str

  primary_key = ('id',)

def setup():
  Note.create_table()

async def create_notes():
  await Note.insert(
    Note(id=1, text="hello"),
    Note(id=2, text="world"),
  )

async def get_notes():
  return await TestSet.select()
```

# Kalessin is not

- serious
- supported
