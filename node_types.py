from pydantic import BaseModel,Field
class Job(BaseModel):
    text:str=Field(min_length=1,max_length=100000); fragments:int=Field(default=6,ge=1,le=100); delay:float=Field(default=0,ge=0,le=60)
