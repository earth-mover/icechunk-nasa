from icechunk import IcechunkStore, StorageConfig, StoreConfig, S3Credentials, VirtualRefConfig
import os

def create_virtual_ds(dmrpps: list[str]):
    # Note: by changing the backend to dmrpp and using the dmrpp files, we speed up generating the virtual dataset by ~100x.
    # To test that out, you can switch filetype=dmrpp to backend=HDFVirtualBackend or remove it altogether to test the kerchunk backend.
    vdss = [
        open_virtual_dataset(
            f, 
            indexes={}, 
            filetype='dmrpp'
        ) for f in dmrpps
    ]
    return xr.concat(
        vdss,
        dim="time",
        coords="minimal",
        compat="override",
        combine_attrs="override",
    )    

def list_mur_sst_files(date_prefix="2002060", fs=None):
    """
    list all files in s3 with a certain date prefix
    """
    mur_sst_files = fs.glob(
        f"s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/{date_prefix}*-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"
    )
    
    return sorted(["s3://" + f for f in mur_sst_files])

def find_or_create_icechunk_store(store_name: str = None, store_type: str = 'local', overwrite = True):
    if store_type == "local":
        directory_path = f"./{store_name}"
        storage_config = StorageConfig.filesystem(directory_path)
        virtual_ref_store_config = StoreConfig(
            virtual_ref_config=VirtualRefConfig.s3_from_env(),
        )        
        if overwrite:
            if os.path.exists(directory_path) and os.path.isdir(directory_path):
                shutil.rmtree(directory_path)  # Removes non-empty directories
                print(f"Directory '{directory_path}' and its contents deleted.")
            else:
                print(f"Directory '{directory_path}' does not exist.")    
            store = IcechunkStore.create(
                storage=storage_config, config=virtual_ref_store_config, read_only=False
            )
        else:
            store = IcechunkStore.open_existing(
                storage=storage_config, config=virtual_ref_store_config, read_only=False
            )            
    # Create a session with the EC2 instance's attached role
    if store_type == "s3":
        session = boto3.Session()
        
        # Get the credentials from the session
        credentials = session.get_credentials()
        
        # Extract the actual key, secret, and token
        creds = credentials.get_frozen_credentials()
        storage = StorageConfig.s3_from_config(
            bucket='nasa-veda-scratch',
            prefix=f"icechunk/{store_name}",
            region='us-west-2',
            credentials=S3Credentials(
                access_key_id=creds.access_key,
                secret_access_key=creds.secret_key,
                session_token=creds.token            
            )    
        )
        if overwrite == True:
            store = IcechunkStore.create(
                storage=storage, 
                config=StoreConfig()
            )
        else:
            store = IcechunkStore.open_existing(storage=storage, config=StoreConfig(), read_only=False)
    if store_type == "array_lake":
        from arraylake import Client
        client = Client()
        client.login()
        store = client.get_or_create_repo(f"nasa-impact/{store_name}", kind="icechunk")
    return store


def split_list(numbers, specific_number):
    if specific_number not in numbers:
        raise ValueError(f"The specific number {specific_number} is not in the list.")
    
    # Find the index of the specific number
    index = numbers.index(specific_number)
    
    # Split into three lists
    before = numbers[:index]
    middle = [numbers[index]]  # List containing only the specific number
    after = numbers[index + 1:]
    
    return before, middle, after


def check_codecs(vdss: list):
    from virtualizarr.codecs import get_codecs
    from virtualizarr.manifests.utils import check_same_codecs
    from virtualizarr import zarr
    
    first_codec = get_codecs(vdss[0].analysed_sst.data)
    
    for vds in vdss:
        codec = get_codecs(vds.analysed_sst.data)
        if codec != first_codec:
            print(vds.analysed_sst.encoding)    
