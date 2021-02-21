/*RELACION N:M ----> RELACIÓN MUCHOS A MUCHOS. A MUCHOS PRODUCTOS
LES CORRESPONDE MUCHAS CATEGORIAS*/

/*CONSULTA SIMPLE PARA OBTENER LAS CATEGORIAS Y PRODUCTOS QUE PERTENECEN A CADA UNA DE 
ELLAS.*/
db.categorias2.aggregate(
    [
        {
            $lookup: {
                from: "almacen2",
                localField: "cod",
                foreignField: "cod",
                as: "Productos"
            }
        }]).pretty()

/*En esta coonsulta procedemos en primer lugar a anexar las categorias con cada uno de los productos que le corresponde 
se darán repeticiones ya que a un mismo producto le corresponde varias categorias
. A continuación realizamos un set conjugado con el uso de un $arrayElemAt con el fin de que 
en la siguiente etapa podamos realizar operaciones con camposs provenientes de la colección que
contiene los productos.
En último lugar realizamos un $project en el cual sseleccionamos el nombre de la categoria
, el producto correspondiente, si es apto para veganos y si llleva gluten y el dia, mes y año
en el que el producto llego al almacen*/

db.categorias2.aggregate(
    [
        {
            $lookup: {
                from: "almacen2",
                localField: "cod",
                foreignField: "cod",
                as: "Productos"
            }
        },
        {
            $set: {
                Producto: { $arrayElemAt: ["$Productos", 0] }
            }
        },
        {
            $project: {
                Categoria: "$nombreCategoria",
                Producto: "$Producto.producto",
                AptoVeganos: "$vegano",
                LlevaGluten: "$gluten",
                DistribuidoPor: "$Producto.Distribuidora",
                DiaLlegada: { $dayOfMonth: "$Producto.fecha" },
                MesLlegada: { $month: "$Producto.fecha" },
                AñoLlegada: { $year: "$Producto.fecha" }
            }
        }
    ]
).pretty()

/*En esta consulta comenzamos uniendo a los productos las categorias que les corresponden
a continuación nos que quedamos únicamente con los productos distribuidos por la distribuidora
número 1, y en último lugar proyectamos el nombre del producto en combinación con los paises desde los que es distribuido,
el impuesto que se le aplica en cada país, el precio por cada unidad que se vende,
el número de unidades disponibles y el beneficio bruto de la totalidad de las ventas */

db.almacen2.aggregate(
    [
        {
            $lookup: {
                from: "categorias2",
                localField: "cod",
                foreignField: "cod",
                as: "DetallesProducto"
            }
        },
        {
            $match: {
                Distribuidora: 1
            }
        },
        {
            $project: {
                _id: 0,
                Producto: "$producto",
                PaisOrigen: "$DetallesProducto.paisOrigenProductos",
                ImpuestoAplicado: "$DetallesProducto.ImpuestoAduanas",
                precioDeLaUnidadVendida: "$precioVENT",
                UnidadesDisponibles: "$unidades",
                BeneficioBrutoTotalidadVentas: { $multiply: ["$precioVENT", "$unidades"] },
            }
        }
    ]
).pretty()