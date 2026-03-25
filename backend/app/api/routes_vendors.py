from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder

from app.services.data_cache_service import get_data_cache_service

router = APIRouter()


@router.get("/vendors")
def list_vendors():
    try:
        cache = get_data_cache_service()
        return {"success": True, "data": jsonable_encoder(cache.get_vendors())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch vendors: {str(e)}")


@router.get("/vendors/{vendor_id}/records")
def vendor_records(vendor_id: str):
    try:
        cache = get_data_cache_service()
        return {"success": True, "data": jsonable_encoder(cache.get_vendor_records(vendor_id))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch vendor records: {str(e)}")


@router.get("/vendors/{vendor_id}/paths")
def vendor_paths(vendor_id: str):
    try:
        cache = get_data_cache_service()
        return {"success": True, "data": jsonable_encoder(cache.get_vendor_paths(vendor_id))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch vendor paths: {str(e)}")


@router.get("/invoices/{invoice_id}")
def invoice_case(invoice_id: str):
    try:
        cache = get_data_cache_service()
        data = cache.get_invoice_case(invoice_id)
        if not data:
            return {"success": True, "data": {}}
        return {"success": True, "data": jsonable_encoder(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch invoice case: {str(e)}")
