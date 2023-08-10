from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.exc import ProgrammingError
from os import environ
from typing import Union # esto lo usamos para declarar que un atributo puede de una o n cantidad de types of values

# 3 
# hay un problema y es que si hay un campo  que no existe, este dara error
# lo mejor seria que se lo salte y que no de error
# lo que busque los valores de .env no me da mucha confianza, mejor que el usurario ingrese su vaina

# 3 
# estoy manejando los errores con status y yo no entiendo un carajo sobre es
# documentate bien para poder retornar bien los status

class Db():
    
    def __init__(self, url: str = None, debug: bool = None) -> None:
        
        """
        Parametros de la clase
        """
        
        if not(url):
            #3 no estoy seguro que lea los env
            self.url:str = environ.get('DATABASE_URL') # url para la conection a la db
        else:
            self.url = url
        
        if not(debug):
            #3 no estoy seguro que lea los env
            self.debug:bool= bool(environ.get('DEBUG')) # var booleana para hacer activar el debug
        else:
            self.debug = debug
        
        if self.debug:
            # si estamos en debug notificamos si la connection a la db es exitosa
            try:
                self.engine = create_engine(self.url, echo=self.debug) # establece la conection
                conn = self.engine.connect()
                print("Conexión a la base de datos establecida con éxito")
                conn.close()
                
            except Exception as e:
                print("Error al establecer la conexión")
                print(e)
                
        else:
            self.engine = create_engine(self.url, echo=self.debug)


    def debug_validation(self, table_name: str = None, params: Union[list, dict, tuple] = None) -> bool:
        
        """
        funcion creada para que nos regrese los erros en caso de que estemos en debug
        """
        
        if self.debug:
        
            if not(table_name):
                raise Exception("Debes especificar el nombre de la tabla.")
            
            elif table_name not in self.list_table_names():
                raise Exception(f"no existe {table_name} o esta mal escrito.")
            
            if params:
                
                columns_name:list= self.list_column_names(table_name)
                is_columns_incorrect:list= []
                
                if type(params) is list or type(params) is tuple:
                    
                    for param in params:
                        
                        for name in param.keys():
                            
                            if name not in columns_name:
                                is_columns_incorrect.append(name)
                        
                elif type(params) is dict:
                    is_columns_incorrect = [name for name in params.keys() if name not in columns_name]
                    query = f"SELECT * FROM {table_name} WHERE "
                    for key, value in params.items():
                        if type(value) is str:
                            value = f"'{value}'"
                        else:
                            value = f"{value}"

                        query += f"{key}={value} AND "

                    query = query[:-5] 
                    print("query>>>", query)
                    text = self.__execute(query=query, Return=False) 
                    print(text)                   
                else:
                    raise Exception("params solo acepta valores de tipo list, tuple y dict.")
                
                if len(is_columns_incorrect):
                    raise Exception(f"Campos {' '.join(is_columns_incorrect)} no existen o estan mal escrito.")
                
                
                return True
            
            return True
        
        else:
            return True
    
    
    #3 falta revisar
    def __execute(self, query:Union[str, list, tuple], Return:bool =False, test: bool = False) -> Union[dict, list, tuple, bool]:
        """
        funcion para poder ejecutar cualquier query
        O para verificar que este bien escrito.
        """
        
        type_query: str = type(query)
        
        if not(test):
            with self.engine.connect() as con:
                
                if type_query is list or type_query is tuple:
                    
                    if len(query) == 0:
                        print("No puedes ingresar un array vacio.")
                        return False
                    
                    
                    list_data: list = []
                    
                    for i, qry in enumerate(query):
                        
                        # si es una consulta de tipo select para asi retornar los que genere la consulta
                        if "SELECT" in qry or "select" in qry:
                            try:
                                data: list = con.execute(text(qry)).fetchall()
                                len_data: int =  len(data)
                                # en caso de que nos retorne un un array vacio demos por entendido
                                # que la consulta esta bien escrito, pero lo que se quiere buacar
                                # no existe o esta mal escrito.
                                if not(len_data):
                                    con.commit()
                                    list_data.append(
                                                {
                                        "status":401,
                                        "message": f"El valor que esta buscando no existe o esta mal escrito",
                                        "index_error": i,
                                        "query": qry
                                        }
                                    )
                                
                                elif len_data > 1:
                                    con.commit()
                                    list_data.append(data)
                                    
                                elif len_data == 1:
                                    con.commit()
                                    list_data.append(data[0])
                                
                            except:
                                con.commit() 
                                list_data.append(
                                                {
                                        "status":400,
                                        "message": f"El query en el index {i} esta mal escrito.",
                                        "index_error": i,
                                        "query": qry
                                        }
                                    )
                        else:
                            try:
                                con.execute(text(query))
                                con.commit()
                                
                            except TypeError as e:
                                con.commit() 
                                list_data.append(
                                                {
                                        "status":400,
                                        "message": f"El query en el index {i} esta mal escrito.",
                                        "index_error": i,
                                        "query": qry
                                        }
                                    )
                    if not(len(list_data)):
                        
                        return True
                    
                    elif len(list_data) > 1:
                    
                        return list_data
                    
                    else:
                        return list_data[0]
                    
                elif type_query is str:
                    # consultamos si es una consulta de tipo select para asi retornar los que genere la consulta
                    if "SELECT" in query or "select" in query:
                        try:
                            data = con.execute(text(query)).fetchall()
                            len_data = len(data)
                            
                            if not(len_data):
                                con.commit()
                                return {
                                        "status":401,
                                        "message": f"El valor que esta buscando no existe o esta mal escrito",
                                        "query": query
                                        }
                                
                            elif len_data > 1:
                                con.commit()
                                return data
                            
                            elif len_data == 1:
                                con.commit()
                                return data
                            
                        except:
                            con.commit() 
                            return {
                                "status":400,
                                "message": f"El query esta mal escrito.",
                                "query": query
                            }
                    else:
                        try:
                            con.execute(text(query))
                            con.commit()
                        except TypeError as e:
                            print(e)
                            return {
                                "status":400,
                                "message": f"El query esta mal escrito.",
                                "query": query
                            }
                else:
                    print("solo se puede ingresar tuples, arrays o strings en el parametro de query.")
                    return False
        
        # en caso de se ejecute como test
        else:  
            
            with self.engine.begin() as conn:
                
                if type_query is list or type_query is tuple:
                    
                    if len(query) == 0:
                        print("No puedes ingresar un array vacio.")
                        return False
                    
                    error_list: list = []
                    for i, qry in enumerate(query):
                        
                        try:
                            conn.execute(text(qry))

                        except ProgrammingError:
                            conn.rollback()
                            error_list.append(
                                {
                                    "status":400,
                                    "message": f"El query en el index {i} esta mal escrito.",
                                    "index_error": i,
                                    "query": qry
                                }
                            )

                    if not(len(error_list)):
                        return True
                    
                    elif len(error_list) > 1:
                        return False, error_list
                    
                    else:
                        return False, error_list[0]
                        
                elif type_query is str:
                    
                    try:
                        conn.execute(text(query))
                        return True
                    except ProgrammingError:
                        conn.rollback()                        
                        return False
                
                else:
                    print("solo se puede ingresar tuples, arrays o strings en el parametro de query.")
                    return False
    

    def list_table_names(self, schema:str="public") -> list:
        """
        lista los nombre de las columnas que hay en la db
        """
        query:str= f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}' ORDER BY table_name;"
        consultation: tuple = self.__execute(query=query)
        
        if len(consultation):
            return [result[0] for result in consultation]
        else:
            print("no hay tablas o el esquema ingresado o no existe.")
            return False
    
    
    def list_column_names(self, table_name: str = None) -> list:
        """
        lista los nombres de las columnas de una tabla, hay que especificar
        el nombre de la tabla para que la pueda buscar
        """
        
        if not(table_name):
            print("Debes especificar el nombre de la tabla.")
            return
        
        query:str= f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;"
        consultation:tuple= self.__execute(query=query)
        
        if len(consultation):
            return [result[0] for result in consultation]
        else:
            print("no existe la tabla ingresada o esta mal escrita.")
            return False
    
    
    # 3 falta por completar
    # falta por revisar
    # falta hacer que se pueda realizar busquedas por varios parametros y no solo por uno solo con el operador OR o AND
    # tambien si el query es de tipo SELECT le podemos retornar la data ya estructurada para asi poder ayudarlo
    def get(self, table_name:str=None, params: Union[dict, list, tuple] = None, columns_name: list = None) -> list:
        """
        funcion para poder obtener datos de una determinada tabla
        """
        
        if self.debug:
            
            if not(self.debug_validation(table_name=table_name, params=params)):
                return
        
            # si la persona quiere ingresa las columnas que quiere que se le regresen 
            # debemos comprobar que estas esten bien escritas
            if columns_name:
                if not(type(columns_name) is list):
                    print("el parametro de columns_name debe de ser de tipo array con los nombres de las columnas")
                    return False
                    
                columns_name:list= self.list_column_names(table_name)
                is_columns_incorrect:list= []
                
                # comprobamos que que las columnas del params esten bien escritas
                
                type_params = type(params)
                
                if type_params is dict:
                    is_columns_incorrect = [name for name in params.keys() if name not in columns_name]
                    
                elif type_params is list or type_params is tuple:
                    
                    if len(params):
                    
                        for param in params:
                            
                            for name in param.keys():
                                
                                if name not in columns_name:
                                    is_columns_incorrect.append(name)
                    else:
                        print("No puedes ingresar un array o tupla vacia.")
                        return False
                else:
                    print("para hacer uso del atributo params de tipo list, tuple o dict, no se le puede ingresar de otro tipo.")
                    return False
                
                if len(is_columns_incorrect):
                        print(f"{' '.join(is_columns_incorrect)} no existen o estan mal escrito.")
                        return False
        
        # ajustamos el array y si no lo creamos y ajustamos, para asi ingresarlo en las columnas
        # que queremos que nos devuelva el select|
        if columns_name:
            str_search:str= ",".join(columns_name)
        else:
            columns_name:list= self.list_column_names(table_name)
            str_search:str= ",".join(columns_name)
        ###
        
        # si la persona no ingreso un parametro de busqueda quiere decir que quiere que le regresemos todas las filas.
        # mas adelante podemos agregar un parametro para que liste hasta cierta cantidad
        # si es que asi lo quiere el usuario
        if not(params):

            query: str = f"SELECT {str_search} FROM {table_name}"
            data: tuple = self.__execute(query=query)
            
            list_data: list = []
            
            if len(data):
                for dt in data:
                    list_data.append(
                        {
                            key:value for key, value in zip(columns_name, dt)
                        }
                    )
                    
                return list_data
            else:
                # en caso de que no retorno nada solamente retornamos un array emtpy
                print("la columna a la que estamos consultando no posee datos.\n\
                    Se recomida ingresar datos, puedes usar el metodo de post, para ingresar datos.")
                return list_data
        
        ## en caso contrario que de si ingreso un o varios parametros paramtros realizamos la busqueda como el lo pide
        
        # si ingresa un list de dict quiere decir que vamos a buscar por varios tipos de parametros asi que entramos un un ciclo for
        # cada parametro va a ser guardado de un list y ese list va a estar guardado en otro
        # si la busqueda esta mal escrita agregara un dict con el error de esa busqueda.
        
        elif type(params) is list or type(params) is tuple: #3 falta prueba
            
            all_data: list = []
            
            for i, param in enumerate(params):
                key, value = list(param.items())[0]
            
                if type(value) is str:
                    value = f"'{value}'"
                else:
                    value = f"{value}"
                
                query:str= f"SELECT {str_search} FROM {table_name} WHERE {key}={value}"
                data:tuple= self.__execute(query=query)
                
                list_data:list= []
                
                if len(data) == 1:
                    all_data.append(
                            {
                                key:value for key, value in zip(columns_name, data[0])
                            }
                        )
                
                elif len(data) > 1:
                    for dt in data:
                        list_data.append(
                            {
                                key:value for key, value in zip(columns_name, dt)
                            }
                        )
                    all_data.append(list_data)
                    
                else:
                    print(f"la consulta en el index {i} esta mal escrita o no existe en la tabla.")
                    all_data.append(
                        {
                        "status":"401",
                        "mesage": f"la consulta en el index {i} esta mal escrita o no existe en la tabla.",
                        key:value,
                        }
                    )
            return all_data
        
        #3 falta prueba
        elif type(params)is dict:
            
            key, value = list(params.items())[0]
            
            if type(value) is str:
                value = f"'{value}'"
            else:
                value = f"{value}"
            
            query:str= f"SELECT {str_search} FROM {table_name} WHERE {key}={value}"
            data:tuple= self.__execute(query=query)
            
            list_data:list= []
            
            if len(data):
                for dt in data:
                    list_data.append(
                        {
                            key:value for key, value in zip(columns_name, dt)
                        }
                    )
                return list_data
                
            else:
                # en caso de que no retorno nada solamente retornamos un array emtpy
                print("No hay datos en la columna o el parametro de busque no existe o esta mal escrito.")
                answer = {
                            "status":"400",
                            "mesage": f"la consulta en el index {i} esta mal escrita o no existe en la tabla.",
                            key:value,
                        }
                return answer
    
    
    #3 falta revisar
    def post(self, table_name: str = None, params: Union[list, dict] = None ) -> None:
        """
        funcion para poder crear datos de una determinada tabla
        """
        
        if self.debug:
            
            if not(self.debug_validation(table_name, params=params)):
                return
        
        type_params: str = type(params)
        
        if type_params is list:
            
            for param in params:
                
                columns_name:str= ",".join(param.keys())
            
                # generamos una lista de valores formateados según su tipo
                values = []
                for val in param.values():
                    if isinstance(val, str):
                        values.append(f"'{val}'")
                    else:
                        values.append(str(val))

                values = ', '.join(values)
                
                query:str= f"INSERT INTO {table_name}({columns_name}) VALUES({values});"
                self.__execute(query=query)
                
        elif type_params is dict:
            
            columns_name:str= ",".join(params.keys())
            
            # generamos una lista de valores formateados según su tipo
            values = []
            for val in params.values():
                if isinstance(val, str):
                    values.append(f"'{val}'")
                else:
                    values.append(str(val))

            values = ', '.join(values)
            
            query:str= f"INSERT INTO {table_name}({columns_name}) VALUES({values});"
            self.__execute(query=query)
            
        else:
            print("el tipo de dato ingresado no es valido debe ser un array un un dict")
            return False
    
    #3
    # falta hacer que se pueda realizar busquedas por varios parametros y no solo por uno solo con el operador OR o AND
    def put(self, table_name: str = None, params:Union[list, dict, tuple] = None, search_column: str = None ) -> None:  
        """
        funcion para actualizar los datos de una determinada tabla, para ello debemos 
        espesificar el nombre de la columna vamos a buscar el valor
        """
        
        if self.debug:
            
            if not(self.debug_validation(table_name=table_name, params=params)):
                return
            
            if not(search_column):
                print("Se debe definir el nombre de la columna donde vamos a realizar la busqueda de la fila")
                return False
            
            if search_column not in self.list_column_names(table_name):
                print("el search_column esta mal escrito o no existe.")
                return False
        
        if type(params) is list:
            
            for param in params:

                assignments = []
                for key, value in param.items():
                    
                    # no vamos a actualizar la columna que estamos usando como referencia para que se actualize tambien
                    if key == search_column: 
                        continue 
                    elif isinstance(value, str):
                        assignments.append(f"{key} = '{value}'")
                    else:
                        assignments.append(f"{key} = {str(value)}")
                            
                # Concatenamos la lista de asignaciones separadas por coma y espacio
                assignments = ', '.join(assignments)

                # Armamos el query completo
                if type(param[search_column]) is str:
                    query = f"UPDATE {table_name} SET {assignments} WHERE {search_column} = '{param[search_column]}'"
                else:
                    query = f"UPDATE {table_name} SET {assignments} WHERE {search_column} = {param[search_column]}"
                
                self.__execute(query=query)
            
        if type(params) is dict:
            
            assignments = []
            for key, value in params.items():
                
                # no vamos a actualizar la columna que estamos usando como referencia para que se actualize tambien
                if key == search_column: 
                    continue 
                elif isinstance(value, str):
                    assignments.append(f"{key} = '{value}'")
                else:
                    assignments.append(f"{key} = {str(value)}")
                    
            # Concatenamos la lista de asignaciones separadas por coma y espacio
            assignments = ', '.join(assignments)

            # Armamos el query completo
            if type(params[search_column]) is str:
                query = f"UPDATE {table_name} SET {assignments} WHERE {search_column} = '{params[search_column]}'"
            else:
                query = f"UPDATE {table_name} SET {assignments} WHERE {search_column} = {params[search_column]}"
            
            self.__execute(query=query)
    
    #3 Falta por revisar
    # falta hacer que se pueda realizar busquedas por varios parametros y no solo por uno solo con el operador OR o AND
    def delete(self, table_name: str = None, field_name:str = None, value:Union[str, int] = None) -> None:
        
        """delete

        Args:
            table_name (str): nombre de la tabla
            field_name (str): nombre del campo en la tabla
            value (str): valor con el que se encuentra almacenado
        """
        
        if self.debug:
            #validamos que todos los paramétros
            #vengan con data
            if not table_name:
                raise Exception("El table_name no puede estar vacío")
            if not field_name and not value:
                raise Exception("El field_name y el value no puede estar vacío")
            if not field_name:
                raise Exception("El field_name no puede estar vacío")
            if not value:
                raise Exception("El value no puede estar vacío")
            
            if self.__validate_field_name(table_name=table_name, field_name=field_name):
                #Construir params para validar 
                params={field_name: value}
                if not(self.debug_validation(table_name=table_name, params=params)):
                    return 
                query:str= f"DELETE FROM {table_name} WHERE {field_name} = {value};"
                self.__execute(query=query, Return=False)


    def __validate_field_name(self, table_name:str, field_name:str):
        """__validate_field_name

        Args:
            table_name (str): nombre de la tabla
            field_name (str): nombre del campo en la tabla

        Returns:
            boolean: si el campo esta dentro de la tabla
        """
        columns_name:list= self.list_column_names(table_name)
        print(columns_name)
        if field_name in columns_name:
            return True
        else:
            raise Exception("El field_name no pertenece a esta tabla")
