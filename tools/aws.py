import boto3
import os

class Aws():
    
    "establece una coneccion a un Bucket para poder realizar consultas a este"
    
    def __init__(self, access_id: str = None, secret_key: str = None,\
        bucket_name: str = None, object_name: str = None) -> None:
        
        # comprobamos el id
        if access_id:
            self.access_id: str = access_id
        else:
            # en caso de que no buscamos en el .env
            self.access_id: str = os.environ.get('AWS_ACCESS_KEY_ID')
            
            if not(self.access_id):
                # si no existe mandamos una alerta
                print("Es necesario ingresar un AWS_ACCESS_KEY_ID\no declararlo en env.")
                
        # comprobamos el password
        if secret_key:
            self.secret_key: str = secret_key
        else:
            # en caso de que no buscamos en el .env
            self.secret_key: str = os.environ.get('AWS_SECRET_ACCESS_KEY')
            
            if not(self.secret_key):
                # si no existe mandamos una alerta
                print("Es necesario ingresar un AWS_SECRET_ACCESS_KEY\no declararlo en env.")
        
        try:
            #Creating Session With Boto3.
            self.session = boto3.Session(
                aws_access_key_id = self.access_id,
                aws_secret_access_key = self.secret_key
                )
            
            #Creating S3 Resource From the Session.
            self.s3 = self.session.resource('s3')
            
            # si este metodo da error da error sabremos que la conection no fue buena
            self.list_bucket()
            
            if bucket_name:
                #Bocket that we are using
                self.bucket = self.s3.Bucket(bucket_name)
                
                if object_name:
                    self.object = self.bucket.Object(f"{object_name}/")
                else:
                    self.object = None
            else:
                #Bocket that we are using
                bucket_name: str = os.environ.get('BUCKET_NAME')
                
                if bucket_name:
                    self.bucket = self.s3.Bucket(bucket_name)
                    
                    if object_name:
                        self.object = self.bucket.Object(object_name)
                    else:
                        self.object = None
                    
                else:
                    print("Es necesario ingresar un Bucket, en caso de no saber cual usar\npuedes usar el metodo 'list_buckets' para escoger con el que quieres trabajar.")
                    self.bucket = None
                
        except:
            
            print("El AWS_ACCESS_KEY_ID o el AWS_SECRET_ACCESS_KEY esta mal escrito y no se puede establecer la coneccion al s3.")
    
    
    def list_bucket(self) -> list:
        
        """Retorna todos los bockets que hay en el s3."""
        
        buckets = self.s3.buckets.all()
        bucket_list: list = [bucket.name for bucket in buckets]
        
        return bucket_list
    
    
    def create_object(self, object_name: str = None) -> None:
        
        """Crea un objeto en el bucket."""
        
        if self.bucket:
            self.bucket.Object(f"{object_name}/").put(Body=b'')
            
        else:
            print("Es necesario ingresar el nombre del bucket.")
    
    
    def delete_object(self, object_name: str = None) -> None:
        
        """Elimina el objeto de un bucket."""
        
        if self.bucket:
            
            if self.object:
                self.object.delete()
                
            elif object_name:
                object = self.bucket.Object(object_name)
                object.delete()
                
            else:
                print("Es necesario declarar el nombre del objeto que se va borrar.")
                
        else:
            print("Es necesario ingresar el nombre del bucket.")
    
    
    def list_object(self, objectName: str = None) -> list:
        
        """lista los objetos de una bucket"""
        
        if self.bucket_name:
            objects = self.bucket.objects.all()
            list_object = [object.key for object in objects]
            return list_object
        else:
            print("Es necesario declarar el nombre del bucket.")
    
    
    def upload_file(self, address_file: str = None ) -> None:
        
        # obtenemos la direccion y la separamos para obtner
        # nombre del archivo.
        file_name: str = os.path.basename(address_file)
        
        if self.bucket_name:
            if self.object_name:
                try:
                    self.object.put(Body= open(address_file, "rb"), Key = file_name)
                    print(f"Acaba de ser ingresado el archivo {file_name} en el objeto {self.object_name} en el bucket {self.bucket_name}.")
                except TypeError as e:
                    print(e)
            else:
                print("Es necesario ingresar el nombre del objeto que esta dentro del bucket donde vamos a ingresar el archivo.")
        else:
            print("Es necesario ingresar el nombre del Bucket donde vamos a ingresar el archivo.")
    
    
    def download_file(self) -> None:
        pass