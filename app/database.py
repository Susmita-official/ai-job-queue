from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base

# 1. The Connection String
# WHY: This acts as the map and password for Python to find the Docker database.
DATABASE_URL = "postgresql+asyncpg://admin:supersecretpassword@localhost:5432/jobqueue"

# 2. The Engine
# WHY: The engine is the actual "physical cable" connecting our app to PostgreSQL.
# (Note: I added `echo=True`. This is a great trick for beginners! It tells SQLAlchemy to print all 
#the secret SQL commands it writes into your terminal so you can watch it work).
engine = create_async_engine(DATABASE_URL, echo=True, poolclass=NullPool)

# 3. The Session Maker
# WHY: A database "Session" is a temporary workspace. When a user submits a job, we will ask this factory to create a new session, 
#put the job inside, and "commit" (save) it to the database safely.
AsyncSessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# 4. The Base Blueprint
# WHY: We need to build a Database Table to hold our jobs. This Base is a magical master class. In our next step, 
#we will tell our Job table to inherit from this Base, which handles all the messy database translation for us.
Base = declarative_base()