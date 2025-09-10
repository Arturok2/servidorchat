# -*- coding: utf-8 -*-
# Se añaden imports de __future__ para compatibilidad. DEBEN ESTAR AL PRINCIPIO.
from __future__ import print_function, unicode_literals

import socket
import threading
import json
import datetime
import time
import traceback
import platform
import os
import logging
import sys
import io

# --- Configuración de Logging para Cloud ---
logger = logging.getLogger('ServidorChat')
logger.setLevel(logging.INFO)

# En lugar de archivo, usar stdout para que los logs sean visibles en la plataforma
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)
# --- Fin Configuración de Logging ---

# Usar variables de entorno para la configuración
HOST = os.environ.get('HOST', '0.0.0.0')  # 0.0.0.0 para escuchar en todas las interfaces
PORT = int(os.environ.get('PORT', 3074))  # Puerto dinámico desde variable de entorno

# Para persistencia en cloud, usaremos variables en memoria y opcionalmente una base de datos
ARCHIVO_TIENDAS_REGISTRADAS = "tiendas_registradas.json"

class Servidor:
    def __init__(self, host=HOST, puerto=PORT):
        self.host = host
        self.puerto = puerto
        self.servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.servidor.bind((self.host, self.puerto))
        except (OSError, socket.error) as e:
            logger.critical("Error Fatal al hacer bind en {}:{} - {}".format(self.host, self.puerto, e), exc_info=True)
            logger.critical("Verifica que la direccion IP sea correcta para esta maquina y que el puerto no este en uso.")
            raise
        self.servidor.listen(10)
        self.clientes = {}
        self.clientes_lock = threading.Lock()
        
        self.tiendas_persistentes = {} 
        self.todas_las_tiendas_conocidas = set()
        self.cargar_tiendas_registradas() 

        logger.info("Servidor iniciado en {}:{}".format(host, puerto))
        print("Servidor iniciado en {}:{}".format(host, puerto))

    def _enviar_json(self, sock, data_dict):
        """Funcion auxiliar para enviar JSON de forma compatible con Py2 y Py3."""
        try:
            mensaje_unicode = json.dumps(data_dict, ensure_ascii=False) + '\n'
            mensaje_bytes = mensaje_unicode.encode('utf-8')
            
            # Usar send() con verificación como en mensaje_privado
            bytes_sent = sock.send(mensaje_bytes)
            if bytes_sent == 0:
                raise socket.error("Socket connection broken")
            elif bytes_sent < len(mensaje_bytes):
                # Si no se enviaron todos los bytes, intentar enviar el resto
                remaining = mensaje_bytes[bytes_sent:]
                sock.send(remaining)
                
        except (socket.error, OSError, ConnectionResetError, BrokenPipeError):
            logger.warning("Error en _enviar_json al enviar a socket")
            raise
        except Exception as e:
            logger.error("Error inesperado en _enviar_json: {}".format(e))
            raise

    def cargar_tiendas_registradas(self):
        """Carga la informacion de las tiendas desde el archivo JSON o variables de entorno."""
        try:
            # Intentar cargar desde variable de entorno primero (para cloud)
            tiendas_env = os.environ.get('TIENDAS_DATA')
            if tiendas_env:
                try:
                    data_cargada = json.loads(tiendas_env)
                    if isinstance(data_cargada, dict):
                        self.tiendas_persistentes = data_cargada
                        self.todas_las_tiendas_conocidas = set(self.tiendas_persistentes.keys())
                        logger.info("Cargadas {} tiendas desde variable de entorno".format(len(self.tiendas_persistentes)))
                        return
                except (ValueError, json.JSONDecodeError) as e:
                    logger.warning("Error parsing TIENDAS_DATA env var: {}".format(e))
            
            # Fallback al archivo local
            if os.path.exists(ARCHIVO_TIENDAS_REGISTRADAS):
                with io.open(ARCHIVO_TIENDAS_REGISTRADAS, 'r', encoding='utf-8') as f:
                    data_cargada = json.load(f)
                    if isinstance(data_cargada, dict): 
                        self.tiendas_persistentes = data_cargada
                    else:
                        logger.warning("{} no contenia un diccionario. Iniciando vacio.".format(ARCHIVO_TIENDAS_REGISTRADAS))
                        self.tiendas_persistentes = {}
                self.todas_las_tiendas_conocidas = set(self.tiendas_persistentes.keys())
                logger.info("Cargadas {} tiendas desde {}".format(len(self.tiendas_persistentes), ARCHIVO_TIENDAS_REGISTRADAS))
            else:
                logger.info("Archivo {} no encontrado. Iniciando con datos vacíos.".format(ARCHIVO_TIENDAS_REGISTRADAS))
                self.tiendas_persistentes = {}
                self.todas_las_tiendas_conocidas = set()
                
        except (ValueError, json.JSONDecodeError):
            logger.error("Error: El archivo {} esta corrupto. Se creara uno nuevo.".format(ARCHIVO_TIENDAS_REGISTRADAS))
            self.tiendas_persistentes = {} 
            self.todas_las_tiendas_conocidas = set()
        except Exception as e:
            logger.error("Error cargando tiendas registradas: {}".format(e), exc_info=True)
            self.tiendas_persistentes = {}
            self.todas_las_tiendas_conocidas = set()

    def guardar_tiendas_registradas(self):
        """Guarda la informacion de las tiendas en el archivo JSON (solo si es posible)."""
        try:
            with self.clientes_lock: 
                datos_a_guardar = self.tiendas_persistentes.copy()

            # Intentar guardar en archivo (puede fallar en algunos entornos cloud)
            try:
                with io.open(ARCHIVO_TIENDAS_REGISTRADAS, 'w', encoding='utf-8') as f:
                    json.dump(datos_a_guardar, f, indent=4, ensure_ascii=False)
                logger.info("Guardadas/actualizadas {} tiendas en {}".format(len(datos_a_guardar), ARCHIVO_TIENDAS_REGISTRADAS))
            except (OSError, PermissionError) as e:
                # En algunos entornos cloud no se puede escribir archivos
                logger.warning("No se pudo guardar archivo (normal en cloud): {}".format(e))
                # Los datos siguen en memoria durante la sesión
                logger.info("Datos mantenidos en memoria: {} tiendas".format(len(datos_a_guardar)))
                
        except Exception as e:
            logger.error("Error guardando tiendas registradas: {}".format(e), exc_info=True)

    # ... [El resto de métodos permanecen igual] ...
    def broadcast_lista_usuarios(self):
        with self.clientes_lock:
            clientes_actuales = list(self.clientes.items())
        if not clientes_actuales: return

        sockets_a_eliminar = []
        for cliente_socket, info in clientes_actuales:
            try:
                lista_para_cliente = self.obtener_lista_usuarios(info["nombre"], info["grupo"])
                mensaje = {"tipo": "usuarios", "usuarios": lista_para_cliente, "mi_grupo": info["grupo"]}
                self._enviar_json(cliente_socket, mensaje)
            except Exception:
                sockets_a_eliminar.append(cliente_socket)
        for sock in sockets_a_eliminar:
            self.eliminar_cliente(sock)

    def notificar_cambio_estado_tienda(self, nombre_tienda, estado, ip_tienda="N/A"):
        mensaje_contenido = "Tienda {} se ha {}.".format(nombre_tienda, estado)
        if estado == "conectado" and ip_tienda != "N/A":
             mensaje_contenido = "Tienda {} se ha {}.".format(nombre_tienda, estado)

        logger.info("Notificando cambio de estado: {}".format(mensaje_contenido))
        mensaje_sistema = {"tipo": "sistema", "contenido": mensaje_contenido, "especifico_oficina": True}
        with self.clientes_lock:
            clientes_oficina = [(sock, info) for sock, info in self.clientes.items() if info.get("grupo") == "oficina"]

        sockets_a_eliminar = []
        for sock_oficina, info_oficina in clientes_oficina:
            try:
                mensaje_con_lista = mensaje_sistema.copy()
                lista_para_receptor = self.obtener_lista_usuarios(info_oficina["nombre"], info_oficina["grupo"])
                mensaje_con_lista["usuarios"] = lista_para_receptor
                self._enviar_json(sock_oficina, mensaje_con_lista)
            except Exception:
                sockets_a_eliminar.append(sock_oficina)
        for sock in sockets_a_eliminar:
            self.eliminar_cliente(sock)

    def notificar_nuevo_usuario(self, nombre_nuevo, grupo_nuevo, ip_nuevo="N/A"):
        if grupo_nuevo == "tienda":
            self.notificar_cambio_estado_tienda(nombre_nuevo, "conectado", ip_nuevo)
            self.broadcast_lista_usuarios()
            return

        mensaje_base_contenido = " {} ({}) se ha unido al chat.".format(nombre_nuevo, grupo_nuevo)
        if grupo_nuevo == "oficina" and ip_nuevo != "N/A":
             mensaje_base_contenido = " {} ({}) se ha unido al chat.".format(nombre_nuevo, grupo_nuevo)
        
        logger.info("Notificando nuevo usuario: {} ({}) IP: {}".format(nombre_nuevo, grupo_nuevo, ip_nuevo))
        mensaje_base = {"tipo": "sistema", "contenido": mensaje_base_contenido}
        
        with self.clientes_lock:
            clientes_a_notificar = list(self.clientes.items())
        sockets_a_eliminar = []

        for sock, info in clientes_a_notificar:
            if info.get("nombre") == nombre_nuevo and info.get("grupo") == grupo_nuevo:
                continue 
            puede_recibir = False
            if info["grupo"] == "oficina": puede_recibir = True
            elif info["grupo"] == "tienda" and grupo_nuevo == "oficina": puede_recibir = True
            if puede_recibir:
                try:
                    mensaje_notificacion = mensaje_base.copy()
                    lista_para_receptor = self.obtener_lista_usuarios(info["nombre"], info["grupo"])
                    mensaje_notificacion["usuarios"] = lista_para_receptor
                    self._enviar_json(sock, mensaje_notificacion)
                except Exception:
                    sockets_a_eliminar.append(sock)
        for sock in sockets_a_eliminar: self.eliminar_cliente(sock)
        self.broadcast_lista_usuarios()

    def enviar_lista_usuarios(self, cliente_socket, nombre_cliente, grupo_cliente):
        try:
            lista_actualizada = self.obtener_lista_usuarios(nombre_cliente, grupo_cliente)
            mensaje = {"tipo": "usuarios", "contenido": "Lista inicial", "usuarios": lista_actualizada, "mi_grupo": grupo_cliente}
            self._enviar_json(cliente_socket, mensaje)
        except Exception as e:
            logger.error("Error enviando lista inicial a {}: {}".format(nombre_cliente, e))

    def obtener_lista_usuarios(self, nombre_solicitante, grupo_solicitante):
        lista_final = []
        with self.clientes_lock:
            clientes_conectados_map = {info["nombre"]: info for info in self.clientes.values()}

        for nombre_usr_ofi, data_usr_ofi in clientes_conectados_map.items():
            if data_usr_ofi.get("grupo") == "oficina":
                if nombre_usr_ofi == nombre_solicitante and grupo_solicitante == "oficina": 
                    continue
                lista_final.append({
                    "nombre": nombre_usr_ofi, 
                    "grupo": "oficina", 
                    "conectado": True, 
                    "ip": data_usr_ofi.get("ip", "N/A")
                })
        
        if grupo_solicitante == "oficina":
            nombres_tiendas_persistentes = sorted(list(self.tiendas_persistentes.keys()))

            for nombre_tienda in nombres_tiendas_persistentes:
                conectado_actualmente = nombre_tienda in clientes_conectados_map and \
                                        clientes_conectados_map[nombre_tienda].get("grupo") == "tienda"
                
                ip_tienda = "N/A"
                if conectado_actualmente:
                    ip_tienda = clientes_conectados_map[nombre_tienda].get("ip", "N/A")
                elif nombre_tienda in self.tiendas_persistentes: 
                    ip_tienda = self.tiendas_persistentes[nombre_tienda].get("ip", "N/A")
                
                lista_final.append({
                    "nombre": nombre_tienda, 
                    "grupo": "tienda", 
                    "conectado": conectado_actualmente,
                    "ip": ip_tienda
                })
        
        return lista_final

    def manejar_cliente(self, cliente_socket, direccion_cliente):
        nombre_cliente_local = "Desconocido@{0}".format(direccion_cliente[0])
        grupo_cliente_local = "desconocido"
        ip_cliente = direccion_cliente[0]
        cliente_registrado = False
        TIMEOUT_CLIENTE = 75

        logger.info("Conexion entrante desde: {0}:{1}".format(direccion_cliente[0], direccion_cliente[1]))

        try:
            # --- Bloque de Registro ---
            cliente_socket.settimeout(TIMEOUT_CLIENTE)
            try:
                registro_bytes = cliente_socket.recv(2048)
                if not registro_bytes:
                    logger.warning("Cliente {0} no envio datos de registro.".format(ip_cliente))
                    return
                datos_registro = json.loads(registro_bytes.decode('utf-8'))
            except socket.timeout:
                logger.warning("Timeout esperando registro de cliente {0}.".format(ip_cliente))
                return
            except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error("Error de formato en registro de {0}: {1}".format(ip_cliente, e))
                try: self._enviar_json(cliente_socket, {"tipo": "error", "contenido": "Formato de registro invalido"})
                except Exception: pass
                return
            except Exception as e:
                logger.error("Error recibiendo registro de {0}: {1}".format(ip_cliente, e), exc_info=True)
                return

            tipo_registro = datos_registro.get("tipo")
            nombre_temp = datos_registro.get("nombre")
            grupo_temp = datos_registro.get("grupo")

            if tipo_registro != "registro" or not nombre_temp or not grupo_temp:
                logger.warning("Registro invalido o incompleto de {}: Nombre='{}', Grupo='{}'".format(ip_cliente, nombre_temp, grupo_temp))
                try:
                    self._enviar_json(cliente_socket, {"tipo": "error", "contenido": "Registro invalido o incompleto"})
                except Exception: pass
                return

            nombre_cliente_local = nombre_temp
            grupo_cliente_local = grupo_temp
            
            logger.info("Cliente '{}/{}' (IP: {}) intentando registrarse.".format(nombre_cliente_local, grupo_cliente_local, ip_cliente))

            if grupo_cliente_local == "tienda":
                self.todas_las_tiendas_conocidas.add(nombre_cliente_local)
                self.tiendas_persistentes[nombre_cliente_local] = {
                    "ip": ip_cliente,
                    "ultima_conexion_ts": time.time()
                }
                self.guardar_tiendas_registradas()

            socket_antiguo = None
            with self.clientes_lock:
                for sock, info in list(self.clientes.items()):
                    if info.get("nombre") == nombre_cliente_local:
                        logger.info("Cliente {} reconectandose. Cerrando conexion antigua.".format(nombre_cliente_local))
                        socket_antiguo = sock
                        self.clientes.pop(socket_antiguo, None)
                        break
                self.clientes[cliente_socket] = {"nombre": nombre_cliente_local, "grupo": grupo_cliente_local, "ip": ip_cliente} 
                cliente_registrado = True
            
            logger.info("Cliente '{}/{}' (IP: {}) registrado exitosamente.".format(nombre_cliente_local, grupo_cliente_local, ip_cliente))

            if socket_antiguo:
                try: socket_antiguo.shutdown(socket.SHUT_RDWR)
                except Exception: pass
                try: socket_antiguo.close()
                except Exception: pass

            self.notificar_nuevo_usuario(nombre_cliente_local, grupo_cliente_local, ip_cliente)
            
            # --- Bucle de recepción de mensajes ---
            buffer = b''
            tipo_recibido_loop = None
            while True:
                try:
                    chunk = cliente_socket.recv(4096)
                    if not chunk:
                        logger.info("Cliente {} ({}) cerro conexion (no chunk).".format(nombre_cliente_local, ip_cliente))
                        break
                    buffer += chunk
                    
                    while buffer:
                        try:
                            buffer_str = buffer.decode('utf-8', errors='replace')
                            datos, index = json.JSONDecoder().raw_decode(buffer_str)
                            bytes_consumidos = len(buffer_str[:index].encode('utf-8'))
                            buffer = buffer[bytes_consumidos:]
                            tipo_recibido_loop = datos.get("tipo")

                            if tipo_recibido_loop == "ping": continue
                            if tipo_recibido_loop == "desconexion":
                                logger.info("Cliente {} ({}) envio desconexion.".format(nombre_cliente_local, ip_cliente))
                                break 

                            if "remitente" not in datos: datos["remitente"] = nombre_cliente_local
                            if "remitente_grupo" not in datos: datos["remitente_grupo"] = grupo_cliente_local
                            if "hora" not in datos: datos["hora"] = self.obtener_hora_actual()
                            datos["remitente_ip"] = ip_cliente 

                            if tipo_recibido_loop == "mensaje":
                                self.broadcast(datos, cliente_socket)
                            
                            elif tipo_recibido_loop == "privado":
                                destinatario = datos.get("destinatario")
                                if destinatario:
                                    enviado = self.mensaje_privado(datos, destinatario, cliente_socket)
                                    confirmacion = {"tipo": "confirmacion_privado", "enviado": enviado, "destinatario": destinatario, "contenido": datos.get("contenido"), "hora": datos.get("hora")}
                                    try: self._enviar_json(cliente_socket, confirmacion)
                                    except Exception: pass
                            
                            elif tipo_recibido_loop == "mensaje_multiple":
                                destinatarios = datos.get("destinatarios", [])
                                contenido = datos.get("contenido", "")
                                if destinatarios and contenido:
                                    logger.info("Recibido mensaje multiple de '{}' para: {}".format(nombre_cliente_local, ", ".join(destinatarios)))
                                    mensaje_a_enviar = {
                                        "tipo": "privado",
                                        "remitente": nombre_cliente_local,
                                        "remitente_grupo": grupo_cliente_local,
                                        "contenido": "(Mensaje Grupal) {}".format(contenido),
                                        "hora": datos.get("hora")
                                    }
                                    for dest in destinatarios:
                                        self.mensaje_privado(mensaje_a_enviar, dest, cliente_socket)
                            
                            elif tipo_recibido_loop == "mensaje_tienda":
                                contenido = datos.get("contenido", "")
                                if contenido:
                                    logger.info("Recibido mensaje masivo para tiendas de '{}'".format(nombre_cliente_local))
                                    mensaje_a_enviar = {
                                        "tipo": "privado",
                                        "remitente": nombre_cliente_local,
                                        "remitente_grupo": grupo_cliente_local,
                                        "contenido": "(MENSAJE MASIVO) {}".format(contenido),
                                        "hora": datos.get("hora")
                                    }
                                    self.broadcast_a_grupo("tienda", mensaje_a_enviar, remitente_socket=cliente_socket)
                            
                            elif tipo_recibido_loop == "solicitar_usuarios":
                                self.enviar_lista_usuarios(cliente_socket, nombre_cliente_local, grupo_cliente_local)
                        
                        except (ValueError, json.JSONDecodeError):
                            break 
                        except Exception as e_inner_loop:
                             logger.error("Error procesando buffer de {}: {}".format(nombre_cliente_local, e_inner_loop), exc_info=True)
                             buffer = b""
                             break
                    
                    if tipo_recibido_loop == "desconexion": break 

                except socket.timeout:
                    logger.info("Timeout para cliente {} ({}). Cerrando conexion.".format(nombre_cliente_local, ip_cliente))
                    break
                except (ConnectionResetError, BrokenPipeError, OSError, socket.error) as e:
                    logger.warning("Error de conexion con {} ({}): {}. Cerrando.".format(nombre_cliente_local, ip_cliente, e))
                    break
                except Exception as e_main_loop:
                    logger.error("Error en bucle principal de {}: {}".format(nombre_cliente_local, e_main_loop), exc_info=True)
                    break
        
        except Exception as outer_err:
            logger.error("Error fatal inicializando manejo para {}: {}".format(direccion_cliente, outer_err), exc_info=True)
        finally:
            if cliente_registrado:
                self.eliminar_cliente(cliente_socket)
                
                if grupo_cliente_local == "tienda" and nombre_cliente_local in self.tiendas_persistentes:
                    self.tiendas_persistentes[nombre_cliente_local]["ultima_conexion_ts"] = time.time() 
                    self.guardar_tiendas_registradas()

            elif cliente_socket:
                try: cliente_socket.close()
                except Exception: pass

    def eliminar_cliente(self, cliente_socket):
        info_cliente = None
        fue_eliminado_ahora = False

        with self.clientes_lock:
            if cliente_socket in self.clientes:
                info_cliente = self.clientes.pop(cliente_socket)
                fue_eliminado_ahora = True
        
        if fue_eliminado_ahora:
            nombre_eliminado = info_cliente.get("nombre", "Desconocido")
            grupo_eliminado = info_cliente.get("grupo", "Desconocido")
            ip_eliminado = info_cliente.get("ip", "N/A")
            logger.info("Cliente '{}/{}' eliminado de la lista de activos.".format(nombre_eliminado, grupo_eliminado, ip_eliminado))
            try: cliente_socket.shutdown(socket.SHUT_RDWR)
            except (OSError, socket.error): pass
            try: cliente_socket.close()
            except Exception: pass
            self.notificar_desconexion(nombre_eliminado, grupo_eliminado, ip_eliminado)

    def notificar_desconexion(self, nombre_desconectado, grupo_desconectado, ip_desconectado="N/A"):
        logger.info("Notificando desconexion de: {} ({})".format(nombre_desconectado, grupo_desconectado, ip_desconectado))
        if grupo_desconectado == "tienda":
            self.notificar_cambio_estado_tienda(nombre_desconectado, "desconectado", ip_desconectado)
            self.broadcast_lista_usuarios() 
            return

        mensaje_contenido = " {} ({}) ha abandonado el chat.".format(nombre_desconectado, grupo_desconectado)
        if grupo_desconectado == "oficina" and ip_desconectado != "N/A":
             mensaje_contenido = " {} ({}) ha abandonado el chat.".format(nombre_desconectado, grupo_desconectado, ip_desconectado)

        mensaje_base = {"tipo": "sistema", "contenido": mensaje_contenido}
        
        with self.clientes_lock:
            clientes_a_notificar = list(self.clientes.items())
        if not clientes_a_notificar: return

        sockets_a_eliminar = []
        for socket_cliente, info_receptor in clientes_a_notificar:
            try:
                mensaje = mensaje_base.copy()
                lista_actualizada = self.obtener_lista_usuarios(info_receptor["nombre"], info_receptor["grupo"])
                mensaje["usuarios"] = lista_actualizada
                mensaje["mi_grupo"] = info_receptor["grupo"]
                self._enviar_json(socket_cliente, mensaje)
            except Exception:
                sockets_a_eliminar.append(socket_cliente)
        for sock in sockets_a_eliminar:
            self.eliminar_cliente(sock) 
        self.broadcast_lista_usuarios()

    def broadcast(self, mensaje_dict, remitente_socket=None):
        remitente_info = None
        with self.clientes_lock:
            if remitente_socket: remitente_info = self.clientes.get(remitente_socket)
            clientes_actuales = list(self.clientes.items())

        sockets_a_eliminar = []
        for cliente_socket, info_receptor in clientes_actuales:
            if cliente_socket == remitente_socket: continue
            if remitente_info and remitente_info.get("grupo") == "tienda" and info_receptor.get("grupo") == "tienda": continue
            try: 
                self._enviar_json(cliente_socket, mensaje_dict)
            except Exception: 
                sockets_a_eliminar.append(cliente_socket)
        for sock in sockets_a_eliminar: self.eliminar_cliente(sock)

    def broadcast_a_grupo(self, grupo_destino, mensaje_dict, remitente_socket=None):
        """Envía un mensaje a todos los miembros de un grupo específico."""
        with self.clientes_lock:
            clientes_del_grupo = [
                (sock, info) for sock, info in self.clientes.items() 
                if info.get("grupo") == grupo_destino and sock != remitente_socket
            ]
        
        sockets_a_eliminar = []
        logger.info("Enviando broadcast al grupo '{}' ({} miembros)".format(grupo_destino, len(clientes_del_grupo)))
        for cliente_socket, info_receptor in clientes_del_grupo:
            try:
                self._enviar_json(cliente_socket, mensaje_dict)
            except Exception:
                sockets_a_eliminar.append(cliente_socket)
        
        for sock in sockets_a_eliminar:
            self.eliminar_cliente(sock)

    def mensaje_privado(self, mensaje_dict, destinatario_nombre, remitente_socket):
        enviado = False
        socket_destinatario = None
        nombre_remitente = "Desconocido"

        with self.clientes_lock:
            remitente_info = self.clientes.get(remitente_socket)
            if remitente_info: nombre_remitente = remitente_info.get("nombre", "Desconocido")
            for sock, info in self.clientes.items():
                if info.get("nombre") == destinatario_nombre:
                    socket_destinatario = sock
                    break
        
        if socket_destinatario:
            try:
                self._enviar_json(socket_destinatario, mensaje_dict)
                enviado = True
            except Exception as e:
                logger.warning("Error enviando mensaje privado de {} a {}: {}. Eliminando destinatario.".format(nombre_remitente, destinatario_nombre, e))
                self.eliminar_cliente(socket_destinatario)
        else: 
            error_msg_content = "Usuario {} no encontrado o desconectado.".format(destinatario_nombre)
            if destinatario_nombre in self.todas_las_tiendas_conocidas:
                 error_msg_content = "Tienda {} esta desconectada.".format(destinatario_nombre)
            if remitente_socket:
                try:
                    error_msg = {"tipo": "error", "contenido": error_msg_content}
                    self._enviar_json(remitente_socket, error_msg)
                except Exception: pass
        return enviado

    def obtener_hora_actual(self):
        return datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    def iniciar(self):
        logger.info("Esperando conexiones...")
        print("Esperando conexiones...")
        try:
            while True:
                try:
                    cliente_socket, direccion = self.servidor.accept()
                    hilo_cliente = threading.Thread(target=self.manejar_cliente, args=(cliente_socket, direccion))
                    hilo_cliente.daemon = True
                    hilo_cliente.start()
                except (OSError, socket.error) as e:
                    if hasattr(e, 'errno') and e.errno in (10038, 9, 22):
                        logger.info("Socket del servidor cerrado o invalido, deteniendo accept().")
                    else: 
                        logger.error("Error de OSError en accept(): {}".format(e), exc_info=True)
                    break 
                except Exception as e:
                    logger.critical("Error critico al aceptar conexion: {}".format(e), exc_info=True)
        except KeyboardInterrupt:
            logger.info("Servidor detenido por el usuario (Ctrl+C)")
            print("\nServidor detenido por el usuario (Ctrl+C)")
        finally:
            logger.info("Cerrando servidor...")
            print("Cerrando servidor...")
            self.guardar_tiendas_registradas()
            with self.clientes_lock: clientes_a_cerrar = list(self.clientes.keys())
            for sock in clientes_a_cerrar:
                try:
                    self._enviar_json(sock, {"tipo": "sistema", "contenido": "El servidor se esta cerrando."})
                    time.sleep(0.05)
                except Exception: pass
                try: sock.shutdown(socket.SHUT_RDWR)
                except Exception: pass
                try: sock.close()
                except Exception: pass
            logger.info("Cerradas {} conexiones de clientes.".format(len(clientes_a_cerrar)))
            print("Cerradas {} conexiones de clientes.".format(len(clientes_a_cerrar)))
            self.servidor.close()
            logger.info("Socket del servidor cerrado.")
            print("Socket del servidor cerrado.")

if __name__ == "__main__":
    print("Iniciando servidor de chat para cloud...")
    logger.info("Iniciando servidor de chat para cloud...")
    
    # Mostrar configuración
    print("Configuración:")
    print("- Host: {}".format(HOST))
    print("- Puerto: {}".format(PORT))
    
    try:
        servidor = Servidor()
        servidor.iniciar()

    except (OSError, socket.error) as e:
        win_error_code = getattr(e, 'winerror', None)
        errno = getattr(e, 'errno', None)
        if errno == 98 or win_error_code == 10048:
            msg = "Error Fatal: El puerto {} ya esta en uso.".format(PORT)
            logger.critical(msg)
            print(msg)
        elif errno == 99 or win_error_code == 10049:
             msg = "Error Fatal: La direccion IP {} no es valida o no esta disponible.".format(HOST)
             logger.critical(msg)
             print(msg)
        else:
             logger.error("Error de OSError/socket.error al iniciar servidor: {}".format(e), exc_info=True)
             print("Error de OSError/socket.error al iniciar servidor: {}".format(e))
             traceback.print_exc()
    except KeyboardInterrupt:
        logger.info("Servidor detenido durante inicio por el usuario.")
        print("\nServidor detenido durante inicio.")
    except Exception as e:
        logger.critical("Error fatal al iniciar servidor: {}".format(e), exc_info=True)
        print("Error fatal al iniciar servidor: {}".format(e))
        traceback.print_exc()
    finally:
        logger.info("Script del servidor finalizado.")

        print("Script del servidor finalizado.")

