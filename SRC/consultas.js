/* UNO A MUCHOS 1:N ----> A UNA ÚNICA CATEGORÍA (COLECCIÓN CATEGORIAS) LE CORRESPONDE VARIOS PRODUCTOS DE LA COLECCIÓN "ALMACEN" */


/*CONSULTA SIMPLE para obtener los equivalentes en ambas colecciones
*/

db.almacen.aggregate(
    [
        {
            $lookup: {
                from: "categorias",
                localField: "cod",
                foreignField: "cod",
                as: "detallesProducto"
            }
        }
    ]
).pretty()

/*En esta consulta aplicado a la colección alamcen en primer lugar comenzamos
juntando los documentos equivalentes de la colección categorias. Y para terminar realizamos un $project
en el que pedimos que aparezca el nombre del producto, dia, mes y año en el que llega al almacen
por separado, distribuidor que trae el producto, pais de origen del producto,
impuesto de aduanas que se le aplica, y si es o no apto para consumo vegano.*/

db.almacen.aggregate(
    [
        {
            $lookup: {
                from: "categorias",
                localField: "cod",
                foreignField: "cod",
                as: "detallesProducto"
            }
        },
        {
            $project: {
                _id: 0,
                producto: "$producto",
                diaQueLLegaAlmacen: { $dayOfMonth: "$fecha" },
                mesQueLLegaAlmacen: { $month: "$fecha" },
                año: { $year: "$fecha" },
                DistribuidoPor: "$Distribuidora",
                PaisOrigen: "$detallesProducto.paisOrigenProductos",
                ImpuestoAplicado: "$detallesProducto.ImpuestoAduanas",
                AptoParaVeganos: "$detallesProducto.vegano"
            }
        }
    ]
).pretty()

/*En esta consulta en la primera etapa utilizamos el operador lookup para de esta manera anexar a los productos de la colección
almacen, los detalles de producto de la colección categorias que le corresponden. A continuación procedemos a aplicar el operador
match con el fin de quedarnos unicamente con aquellos productos cuya fecha de llegada al almacen haya sido 2021. A continuación hacemos uso
 del operador $set en combinación con $arrayElemAt ya que de esta manera podremos realizar operaciones entre campos de diferentes colecciones 
  sin que surjan errores.A continuación realizamos un $project, en el cual calculamos el Beneficio bruto que se obtiene con la unidad de un producto,
  además de el peso que tiene el impuesto de aduanas sobre el producto cuando el almacen los compra. En último lugar procedemos a calcular el beneficio limpio
  de la venta de una unidad de un producto a partir de realización de la diferencia entre el BeneficioBruto y el impuesto aplicado
  a cada unidad  */

db.almacen.aggregate(
    [
        {
            $lookup: {
                from: "categorias",
                localField: "cod",
                foreignField: "cod",
                as: "detallesProducto"
            }
        },
        {
            $match:
            {
                $expr: { $eq: [{ $year: "$fecha" }, 2021] }
            }
        },
        {
            $set: {
                detalles: { $arrayElemAt: ["$detallesProducto", 0] }
            }
        },
        {
            $project: {
                _id: 0,
                Producto: "$producto",
                Categoria: "$detalles.nombreCategoria",
                PaisOrigen: "$detalles.paisOrigenProductos",
                ImpuestoAplicado: "$detalles.ImpuestoAduanas",
                BeneficioBrutoPorUnidadVendida: { $subtract: ["$precioVENT", "$precioCompra"] },
                precioDeLaUnidad: "$precioCompra",
                impuestoPorUnidadComprada: { $sum: { $multiply: ["$precioCompra", "$detalles.ImpuestoAduanas"] }},
                
            }
        },
        {
            $set: {
                BeneficioLimpioPorUnidadVendida: { $subtract: ["$BeneficioBrutoPorUnidadVendida", "$impuestoPorUnidadComprada"] }
            }
        }
    ]
).pretty()