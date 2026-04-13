import uvicorn

if __name__ == "__main__":
    print("CACHE WARMUP TRIGGERED")
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )