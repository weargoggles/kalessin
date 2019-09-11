# Kalessin is:

- an homage to Codd's relational algebra of sets
- a wrapper around @tomchristie's excellent [databases](//github.com/encode/databases)
- an experiment in Python metaclasses. Sets combine the typing.NamedTuple metaclass machinery with something like Django's ModelMeta to get model instances which are:
  - typed
  - tuples in memory
  - loaded and stored with SQLAlchemy

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
