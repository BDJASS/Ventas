@openapi.openedge.export FILE(type="REST", executionMode="single-run", useReturnValue="false", writeDataSetBeforeImage="false").

BLOCK-LEVEL ON ERROR UNDO, THROW.

/* ***************************  Definitions  ************************** */

DEFINE TEMP-TABLE ttVentas NO-UNDO
    FIELD idCliente     AS INTEGER
    FIELD razonSocial   AS CHARACTER
    FIELD totalAnterior AS DECIMAL
    FIELD totalActual   AS DECIMAL
    FIELD crecimiento   AS DECIMAL
    FIELD idaccion      AS CHAR
    FIELD Segmento      LIKE Cliente.Id-Segmento
    FIELD Tel           AS CHAR.
    
DEFINE TEMP-TABLE ttProductoCliente NO-UNDO
    FIELD idCliente     AS INTEGER
    FIELD Codigo        AS CHARACTER
    FIELD idColor       LIKE detfactura.Id-color
    FIELD ColorDesc     AS CHARACTER
    FIELD descripcion   AS CHARACTER
    FIELD totalAnterior AS DECIMAL
    FIELD PropAnterior  AS DECIMAL
    FIELD totalActual   AS DECIMAL  
    FIELD PropActual    AS DECIMAL
    FIELD crecimiento   AS DECIMAL     
    INDEX idx1 Codigo.  
     
DEFINE DATASET dsVentasCliente FOR
    ttVentas,
    ttProductoCliente
    DATA-RELATION drClienteProducto
    FOR ttVentas, ttProductoCliente
    RELATION-FIELDS(idCliente, idCliente)
    NESTED.
    
DEFINE VARIABLE totalActual      AS DECIMAL   NO-UNDO .
DEFINE VARIABLE totalAnterior    AS DECIMAL   NO-UNDO .
/* Variables locales */
DEFINE VARIABLE fechaFin         AS DATE      NO-UNDO.
DEFINE VARIABLE fechaIniActual   AS DATE      NO-UNDO.
DEFINE VARIABLE fechaIniAnterior AS DATE      NO-UNDO.
DEFINE VARIABLE fechaFinAnterior AS DATE      NO-UNDO.

DEFINE VARIABLE cFechaISO        AS CHARACTER NO-UNDO.
DEFINE VARIABLE crecimiento      AS DECIMAL   NO-UNDO.
DEFINE VARIABLE l-accion         AS CHARACTER NO-UNDO.
/* ***************************  Main Procedure  ************************** */

@openapi.openedge.export(type="REST", useReturnValue="false", writeDataSetBeforeImage="false").
PROCEDURE GetVentas:  
    /*------------------------------------------------------------------------------*/
    DEFINE INPUT  PARAMETER ip-idVendedor AS CHARACTER NO-UNDO.
    DEFINE INPUT  PARAMETER ip-fechaFin   AS CHARACTER NO-UNDO.
    DEFINE OUTPUT PARAMETER DATASET FOR dsVentasCliente.


    
    /* Extraer solo la parte de fecha (primeros 10 caracteres) */
    cFechaISO = SUBSTRING(ip-fechaFin, 1, 10).  /* Resultado: "2025-01-15" */

    /* Reorganizar a un formato que DATE() entienda, por ejemplo "01/15/2025" */
    cFechaISO = SUBSTRING(cFechaISO, 9, 2) + "/" +  /* DD */ 
        SUBSTRING(cFechaISO, 6, 2) + "/" +  /* MM */            
        SUBSTRING(cFechaISO, 1, 4).         /* YYYY */

    /* Convertir a tipo DATE */     
    fechaFin = DATE(cFechaISO).
    
    
    /* Log inicial de parámetros */
    LOG-MANAGER:WRITE-MESSAGE("==> GetVentas ejecutado. IdVendedor: " + ip-idVendedor + 
        " FechaFin: " + STRING(fechaFin)).



    fechaIniActual   = DATE(01,01,YEAR(fechaFin)).
    fechaIniAnterior = DATE(01,01,YEAR(fechaFin) - 1 ).
    fechaFinAnterior = DATE(MONTH(fechaFin), DAY(fechaFin), YEAR(fechaFin) - 1).
    
    /* Log inicial */
    LOG-MANAGER:WRITE-MESSAGE(
        "==> GetVentas ejecutado. IdVendedor: " + ip-idVendedor +
        " FechaIniActual: "   + STRING(fechaIniActual) +
        " FechaFinActual: "   + STRING(fechaFin) +
        " FechaIniAnterior: " + STRING(fechaIniAnterior) +
        " FechaFinAnterior: " + STRING(fechaFinAnterior)
        ).       
    /* recorrer clientes por vendedor */
    FOR EACH Cliente WHERE Cliente.Id-Vendedor = ip-idVendedor 
        NO-LOCK:

        /* Reinciar acumuladores por cliente */
        ASSIGN
            totalActual   = 0
            totalAnterior = 0.
    
      
        FOR EACH Factura 
            WHERE Factura.Id-Cliente = Cliente.Id-Cliente 
            AND Factura.FecReg >= fechaIniAnterior 
            AND Factura.FecReg <= fechaFin 
            AND Factura.FecCancel = ? 
            NO-LOCK:

            IF Factura.FecReg >= fechaIniActual THEN
                /* Actual */
                totalActual = totalActual + Factura.SubTotal.
            ELSE IF Factura.FecReg <= fechaFinAnterior THEN
                    /* Anterior */
                    totalAnterior = totalAnterior + Factura.SubTotal.
        /* Si no entra en ninguna, es porque cae en los días posteriores a fechaFinAnterior 
           pero antes de fechaIniActual, así que no la contamos */        
        END.
        
        /* FACTURAS CONTADO */ 
        FOR EACH Remision
            WHERE Remision.Id-Cliente = Cliente.Id-Cliente
            AND Remision.FecReg >= fechaIniAnterior 
            AND Remision.FecReg <= fechaFin 
            AND Remision.FecCancel = ? NO-LOCK:
                   
            IF Remision.FecReg >= fechaIniActual THEN
                /* Actual */
                totalActual = totalActual + Remision.SubTotal.
            ELSE IF Remision.FecReg <= fechaFinAnterior THEN
                    /* Anterior (mismo rango que actual, pero un año antes) */
                    totalAnterior = totalAnterior + Remision.SubTotal.
        /* Si no cumple ninguna, es que está después de fechaFinAnterior pero antes de fechaIniActual,
           y no entra en ningún rango de comparación */                     
        END.        

        IF totalActual > 0 OR totalAnterior > 0 THEN 
        DO:
            
            /* Calcular crecimiento */
            IF totalAnterior = 0 THEN 
            DO:
                IF totalActual = 0 THEN
                    crecimiento = 0.
                ELSE
                    crecimiento = 100.
            END.
            ELSE 
            DO:
                crecimiento = ((totalActual - totalAnterior) / totalAnterior) * 100.
            END.
            
            /* Determinar acción */
            IF crecimiento < 0 THEN
                l-accion = "LLAMAR".
            ELSE IF crecimiento = 100 THEN
                    l-accion = "CRECIMIENTO".
                ELSE
                    l-accion = "MANTENIMIENTO".
            
              
            CREATE ttVentas.
            ASSIGN
                ttVentas.idCliente     = Cliente.Id-Cliente
                ttVentas.razonSocial   = Cliente.RazonSocial
                ttVentas.totalActual   = totalActual
                ttVentas.totalAnterior = totalAnterior
                ttVentas.crecimiento   = crecimiento
                ttVentas.idaccion      = l-accion
                ttVentas.Segmento      = Cliente.Id-Segmento
                ttVentas.Tel           = Cliente.Tel1 + " " + Cliente.Tel2 + " " + Cliente.Tel3.    
        END.
        
      
        

    END.  
    /* Calcular crecimiento y acción por producto */

    RETURN.    
END PROCEDURE.
@openapi.openedge.export(type="REST", useReturnValue="false", writeDataSetBeforeImage="false").
PROCEDURE GetDetalleProducto:
    /*------------------------------------------------------------------------------*/
    DEFINE INPUT  PARAMETER ip-idCliente  AS INT NO-UNDO.
    DEFINE INPUT  PARAMETER ip-fechaFin   AS CHARACTER NO-UNDO.
    DEFINE OUTPUT PARAMETER TABLE FOR ttProductoCliente.


    
    /* Extraer solo la parte de fecha (primeros 10 caracteres) */
    cFechaISO = SUBSTRING(ip-fechaFin, 1, 10).  /* Resultado: "2025-01-15" */

    /* Reorganizar a un formato que DATE() entienda, por ejemplo "01/15/2025" */
    cFechaISO = SUBSTRING(cFechaISO, 9, 2) + "/" +  /* DD */ 
        SUBSTRING(cFechaISO, 6, 2) + "/" +  /* MM */            
        SUBSTRING(cFechaISO, 1, 4).         /* YYYY */

    /* Convertir a tipo DATE */     
    fechaFin = DATE(cFechaISO).
    
    
    /* Log inicial de parámetros */
    LOG-MANAGER:WRITE-MESSAGE("==> GetDetalleProducto. Cliente: " + STRING(ip-idCliente) + 
        " FechaFin: " + STRING(fechaFin)).
   


    fechaIniActual   = DATE(01,01,YEAR(fechaFin)).
    fechaIniAnterior = DATE(01,01,YEAR(fechaFin) - 1 ).
    fechaFinAnterior = DATE(MONTH(fechaFin), DAY(fechaFin), YEAR(fechaFin) - 1).
    
    
    /* Log inicial de parámetros */
    LOG-MANAGER:WRITE-MESSAGE("==> GetDetalleProducto. Cliente: " + STRING(ip-idCliente) + 
        " Fecha Inicial Actual: " + STRING(fechaIniActual) + " Fecha Inicial Anterior: " +  STRING(fechaIniAnterior)).
       
                          
    /* recorrer clientes por vendedor */
    FOR EACH Cliente WHERE Cliente.Id-Cliente = ip-idCliente 
        NO-LOCK:  

        /* Reinciar acumuladores por cliente */
        ASSIGN
            totalActual   = 0
            totalAnterior = 0.
    
      
        FOR EACH Factura 
            WHERE Factura.Id-Cliente = Cliente.Id-Cliente 
            AND Factura.FecReg >= fechaIniAnterior 
            AND Factura.FecReg <= fechaFin 
            AND Factura.FecCancel = ? 
            NO-LOCK:

            IF Factura.FecReg >= fechaIniActual THEN
                /* Actual */
                totalActual = totalActual + Factura.SubTotal.
            ELSE IF Factura.FecReg <= fechaFinAnterior THEN
                    /* Anterior */
                    totalAnterior = totalAnterior + Factura.SubTotal.
            ELSE 
                /* NO ESTA EN EL RANGO, SALTAR FACTURA */
                NEXT.
                
            /* PRODUCTOS DETALLE */
            FOR EACH detfactura NO-LOCK
                WHERE detfactura.Id-Factura = Factura.Id-Factura
                AND detFactura.Importe > 0  
                USE-INDEX Idx-Fac :

                FIND FIRST  ttProductoCliente
                    WHERE  ttProductoCliente.idCliente = Cliente.Id-Cliente
                    AND  ttProductoCliente.Codigo    = detfactura.Id-Articulo
                    AND  ttProductoCliente.IdColor   = detfactura.Id-Color  /* <-- nuevo filtro */
                    NO-ERROR.

                IF NOT AVAILABLE ttProductoCliente THEN 
                DO:
                    FIND FIRST Kolor WHERE Kolor.Id-color = detfactura.Id-Color NO-LOCK NO-ERROR.
                    CREATE ttProductoCliente.
                    ASSIGN
                        ttProductoCliente.idCliente   = Cliente.Id-Cliente
                        ttProductoCliente.Codigo      = detfactura.Id-Articulo
                        ttProductoCliente.IdColor     = detfactura.Id-Color
                        ttProductoCliente.ColorDesc   = IF AVAILABLE Kolor THEN Kolor.Descr ELSE ""
                        ttProductoCliente.descripcion = detfactura.Descr. 
                END.

                IF Factura.FecReg >= fechaIniActual THEN              
                    ttProductoCliente.totalActual = ttProductoCliente.totalActual + detfactura.Importe.
                ELSE
                    ttProductoCliente.totalAnterior = ttProductoCliente.totalAnterior + detfactura.Importe.
            END.              
        END.
        
        /* FACTURAS CONTADO */ 
        FOR EACH Remision
            WHERE Remision.Id-Cliente = Cliente.Id-Cliente
            AND Remision.FecReg >= fechaIniAnterior 
            AND Remision.FecReg <= fechaFin 
            AND Remision.FecCancel = ? NO-LOCK:
                   
            IF Remision.FecReg >= fechaIniActual THEN
                /* Actual */
                totalActual = totalActual + Remision.SubTotal.
            ELSE IF Remision.FecReg <= fechaFinAnterior THEN
                    /* Anterior (mismo rango que actual, pero un año antes) */
                    totalAnterior = totalAnterior + Remision.SubTotal. 
            ELSE 
                /* NO ESTA EN EL RANGO, SALTAR REMISION */
                NEXT.
             
            /* PRODUCTOS DETALLE */
            
            FOR EACH DetRemis 
                USE-INDEX Idx-Rem
                WHERE DetRemis.Id-Remision = Remision.Id-Remision 
                AND DetRemis.Importe > 0 NO-LOCK: 

                FIND FIRST ttProductoCliente
                    WHERE  ttProductoCliente.idCliente = Cliente.Id-Cliente
                    AND  ttProductoCliente.Codigo    = DetRemis.Id-Articulo
                    AND  ttProductoCliente.IdColor   = DetRemis.Id-color  /* <-- nuevo filtro */
                    NO-ERROR.

                IF NOT AVAILABLE ttProductoCliente THEN 
                DO:
                    FIND FIRST Kolor WHERE Kolor.Id-color = DetRemis.Id-color NO-LOCK NO-ERROR.
                    CREATE ttProductoCliente.
                    ASSIGN
                        ttProductoCliente.idCliente   = Cliente.Id-Cliente
                        ttProductoCliente.Codigo      = DetRemis.Id-Articulo
                        ttProductoCliente.IdColor     = DetRemis.Id-color
                        ttProductoCliente.ColorDesc   = IF AVAILABLE Kolor THEN Kolor.Descr ELSE ""
                        ttProductoCliente.descripcion = DetRemis.Descr.
                END.  

                IF Remision.FecReg >= fechaIniActual THEN
                    ttProductoCliente.totalActual = ttProductoCliente.totalActual + DetRemis.Importe .
                ELSE
                    ttProductoCliente.totalAnterior = ttProductoCliente.totalAnterior + DetRemis.Importe.
            END.                      
        END.        

        IF totalActual > 0 OR totalAnterior > 0 THEN 
        DO:
            
            /* Calcular crecimiento */
            IF totalAnterior = 0 THEN 
            DO:
                IF totalActual = 0 THEN
                    crecimiento = 0.
                ELSE
                    crecimiento = 100.
            END.
            ELSE 
            DO:
                crecimiento = ((totalActual - totalAnterior) / totalAnterior) * 100.
            END.
            
            /* Determinar acción */
            IF crecimiento < 0 THEN
                l-accion = "LLAMAR".
            ELSE IF crecimiento = 100 THEN
                    l-accion = "CRECIMIENTO".
                ELSE
                    l-accion = "MANTENIMIENTO".
            
            CREATE ttVentas.
            ASSIGN
                ttVentas.idCliente     = Cliente.Id-Cliente
                ttVentas.razonSocial   = Cliente.RazonSocial
                ttVentas.totalActual   = totalActual
                ttVentas.totalAnterior = totalAnterior
                ttVentas.crecimiento   = crecimiento
                ttVentas.idaccion      = l-accion.    
        END.
        
      
        

    END.  
    /* Calcular crecimiento y acción por producto */
     
      
    FOR EACH ttProductoCliente:
        
        FIND FIRST ttVentas 
            WHERE ttVentas.idCliente = ttProductoCliente.idCliente
            NO-LOCK NO-ERROR.

        /* Calcular proporciones */
        IF AVAILABLE ttVentas THEN 
        DO:
            IF ttVentas.totalAnterior > 0 THEN
                ttProductoCliente.PropAnterior = 
                    (ttProductoCliente.totalAnterior / ttVentas.totalAnterior) * 100.
            ELSE
                ttProductoCliente.PropAnterior = 0.

            IF ttVentas.totalActual > 0 THEN
                ttProductoCliente.PropActual = 
                    (ttProductoCliente.totalActual / ttVentas.totalActual) * 100.
            ELSE
                ttProductoCliente.PropActual = 0.
        END. 
    
    
    
        IF ttProductoCliente.totalActual > 0 OR ttProductoCliente.totalAnterior > 0 THEN 
        DO:

            /* Calcular crecimiento */
            IF ttProductoCliente.totalAnterior = 0 THEN 
            DO:
                IF ttProductoCliente.totalActual = 0 THEN
                    ttProductoCliente.crecimiento = 0.
                ELSE
                    ttProductoCliente.crecimiento = 100.
            END.
            ELSE 
            DO:
                ttProductoCliente.crecimiento = ((ttProductoCliente.totalActual - ttProductoCliente.totalAnterior) 
                    / ttProductoCliente.totalAnterior) * 100.
            END.
        END.
    END.       
    RETURN.    
END PROCEDURE.