import stock_get as sg
import stock_craw as sc

if __name__=='__main__':
    sc.stock_craw_page()
    import asyncio
    async def main():
        await sg.stock_csv_get()
    asyncio.run(main())