import icechunk as ic
import zarr
import boto3
import numpy as np
import xarray as xr
from rich.live import Live
from rich.panel import Panel
from rich.console import Console, Group
from rich.jupyter import JupyterMixin
from rich.ansi import AnsiDecoder
from time import perf_counter, sleep
import plotext as plt


console = Console()


zarr.config.set(
    {
        'threading.max_workers': 16,
        'async.concurrency': 32
    }
)


nt, ny, nx = 1, 180, 360 
chunk_shape = (100, 10, 10)


def get_repo():
    
    store_name = "continuous-update-demo-0"
    session = boto3.Session()

    # Get the credentials from the session
    credentials = session.get_credentials()
    
    # Extract the actual key, secret, and token
    creds = credentials.get_frozen_credentials()
    storage = ic.s3_storage(
        bucket='nasa-veda-scratch',
        prefix=f"icechunk/{store_name}",
        access_key_id=creds.access_key,
        secret_access_key=creds.secret_key,
        session_token=creds.token            
    )

    return ic.Repository.open(storage)


def get_dataset():
    repo = get_repo()
    session = repo.readonly_session("main")
    ds = xr.open_dataset(session.store, zarr_format=3, engine="zarr", consolidated=False)
    return ds


class plotextMixin(JupyterMixin):

    def make_plot(self):
        plt.clf()
        x = self.ds.time.values
        y = self.ds.precipitation[:, 0, 0].values
        if len(x) > 40:
            x = x[-40:]
            y = y[-40:]
        plt.plot(x, y, color="green")        
        plt.plotsize(self.width, self.height)
        plt.theme('clear')
        return plt.build()
    
    def __init__(self, ds, title = ""):
        self.decoder = AnsiDecoder()
        self.ds = ds
        self.title = title

    def __rich_console__(self, console, options):
        self.width = options.max_width or console.width
        self.height = 10
        canvas = self.make_plot()
        self.rich_canvas = Group(*self.decoder.decode(canvas))
        yield self.rich_canvas


def main():

    def generate_group(n):
        tic = perf_counter()
        ds = get_dataset()
        delta_t = perf_counter() - tic
        g = Group(
            Panel("[green] Reader polling"),
            f"   [yellow]n={n:5d}[/]    {delta_t:.3f} s",
            Panel(str(ds)),
            plotextMixin(ds)
        )
        n += 1
        return g
    
    n=0
    with Live(generate_group(0)) as live:
        while True:
            n += 1
            live.update(generate_group(n))




if __name__ == "__main__":
    main()
