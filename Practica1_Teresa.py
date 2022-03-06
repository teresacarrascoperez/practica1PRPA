from multiprocessing import Process
from multiprocessing import BoundedSemaphore, Semaphore, Lock
from multiprocessing import current_process
from multiprocessing import Value, Array
from time import sleep
from random import random,randint


N = 5
K = 10
NPROD = 3
NCONS = 1

def delay(factor = 3):
    sleep(random()/factor)


def add_data(storage, index, data, mutex):
    mutex.acquire()
    try:
        storage[index.value] = data
        delay(6)
        index.value = index.value + 1
    finally:
        mutex.release()


def get_data(storage, index, mutex):
    mutex.acquire()
    try:
        data = storage[0]
        index.value = index.value - 1
        delay(6)
        for i in range(index.value):
            storage[i] = storage[i + 1]
        storage[index.value] = -1
    finally:
        mutex.release()
    return data

#En la función producer almacenamos todos los valores que queramos e indicamos con un -1, el fin de la cola
def producer(lst_almacen, lst_index, lst_empty, lst_non_empty, lst_mutex):
    ultimo = 0
    for v in range(N):
        print (f"producer {current_process().name} produciendo")
        delay(6)
        lst_empty.acquire()
        ultimo = ultimo + randint(0,15)
        add_data(lst_almacen, lst_index, ultimo, lst_mutex)
        lst_non_empty.release()
        print (f"producer {current_process().name} almacenado {v}")
    lst_empty.acquire()
    add_data (lst_almacen, lst_index, -1, lst_mutex)
    lst_non_empty.release()

#Definimos la siguiente función para comprobar si quedan valores a consumir en cada uno de los productores
def quedanNProd (lst_almacen):
    return [lst_almacen[0][0] != -1, lst_almacen[1][0] != -1, lst_almacen[2][0] != -1]

def buscarMin(val1, val2):
    minimo_value = min(val1)
    index = val1.index(minimo_value)
    minimo_index = val2[index]
    return minimo_value , minimo_index


#Devuelve la lista de valores y la lista de índices correspondientes, a partir de la lista almacén
def valuesIndex(resultado, lst_almacen):
    l1 = []
    l2 = []
    if resultado[0]:
        l1.append(lst_almacen[0][0])
        l2.append(0)
    if resultado[1]:
        l1.append(lst_almacen[1][0])
        l2.append(1)
    if resultado[2]:
        l1.append(lst_almacen[2][0])
        l2.append(2)
    return l1 , l2


def minHead(lst_almacen, lst_index, lst_mutex):
    resultado = quedanNProd(lst_almacen)
    l1 , l2 = valuesIndex(resultado, lst_almacen)
    if len(l1)>0:
       min_value , min_index = buscarMin(l1,l2)
       get_data(lst_almacen[min_index], lst_index[min_index], lst_mutex[min_index])
    else:
        min_value = -1
        min_index = -1 
    return min_value , min_index



def consumer(sorted_data, lst_almacen, lst_index, lst_empty, lst_non_empty, lst_mutex):
    for i in range(NPROD):
        lst_non_empty[i].acquire()
    quedanProductores = len(quedanNProd(lst_almacen))
    i = 0
    while quedanProductores > 0: #comprobamos si tenemos valores disponibles para consumir en alguno de los productores
        min_value , min_index = minHead(lst_almacen, lst_index, lst_mutex)
        if min_value == -1:
            break
        else:
            print (f"consumer {current_process().name} desalmacenando")
            sorted_data[i] = min_value 
            lst_empty[min_index].release()
            lst_non_empty[min_index].acquire()
            print (f"consumer {current_process().name} consumiendo {min_value}")
            i += 1
            quedanProductores = len(quedanNProd(lst_almacen))


def main():
    sorted_data= Array('i',N*NPROD)
    lst_almacen = [Array('i',K) for i in range (NPROD)]
    lst_index = [Value('i', 0) for i in range (NPROD)]
    lst_non_empty= [Semaphore(0) for i in range(NPROD)]
    lst_empty = [BoundedSemaphore(K) for i in range(NPROD)]
    lst_mutex = [Lock() for i in range(NPROD)]

    prodlst = [ Process(target=producer,
                        name=f'prod_{i}',
                        args=(lst_almacen[i], lst_index[i], lst_empty[i], lst_non_empty[i], lst_mutex[i]))for i in range (NPROD)] 
              
    conslst = [ Process(target=consumer,
                      name=f"cons_{i}",
                      args=(sorted_data,lst_almacen, lst_index, lst_empty, lst_non_empty, lst_mutex)) for i in range (NCONS)] 
                
    for p in prodlst + conslst:
        p.start()

    for p in prodlst + conslst:
        p.join()

    print(sorted_data[:])

if __name__ == '__main__':
    main()
