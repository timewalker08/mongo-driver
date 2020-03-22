_class_registry_cache = {}
_field_list_cache = []


def _import_class(cls_name):
    """Cache mechanism for imports.
    """
    if cls_name in _class_registry_cache:
        return _class_registry_cache.get(cls_name)

    doc_classes = ('Document', 'EmbeddedDocument')

    # Field Classes
    if not _field_list_cache:
        from mongo_driver.fields import __all__ as fields
        _field_list_cache.extend(fields)
        from mongo_driver.base.fields import __all__ as fields
        _field_list_cache.extend(fields)

    field_classes = _field_list_cache

    if cls_name == 'BaseDocument':
        from mongo_driver.base import document as module
        import_classes = ['BaseDocument']
    elif cls_name in doc_classes:
        from mongo_driver import document as module
        import_classes = doc_classes
    elif cls_name in field_classes:
        from mongo_driver import fields as module
        import_classes = field_classes
    else:
        raise ValueError('No import set for: ' % cls_name)

    for cls in import_classes:
        _class_registry_cache[cls] = getattr(module, cls)

    return _class_registry_cache.get(cls_name)
