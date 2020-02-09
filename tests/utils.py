from aiomongoengine import Document


async def get_as_son(document: Document) -> dict:
    new_obj = await document.__class__.objects.get(id=document.id)
    return new_obj.to_son()
