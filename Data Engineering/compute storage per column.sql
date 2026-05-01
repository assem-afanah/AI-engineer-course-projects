DECLARE @SchemaName SYSNAME = 'buisnessDB';
DECLARE @TableName  SYSNAME = 'companies';
DECLARE @ColumnName SYSNAME = 'company_id';


DECLARE 
    @DataType SYSNAME,
    @MaxLength INT,
    @Precision INT,
    @Scale INT,
    @DeclaredLength INT;

-- Get column metadata
SELECT 
    @DataType = ty.name,
    @MaxLength = c.max_length,
    @Precision = c.precision,
    @Scale = c.scale,
    @DeclaredLength = CASE 
                        WHEN ty.name IN ('nvarchar','nchar') 
                            THEN c.max_length / 2
                        ELSE c.max_length
                      END
FROM sys.columns c
JOIN sys.types ty ON c.user_type_id = ty.user_type_id
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName
  AND t.name = @TableName
  AND c.name = @ColumnName;

DECLARE @SQL NVARCHAR(MAX);

SET @SQL = '
SELECT 
    ''' + @SchemaName + '.' + @TableName + ''' AS TableName,
    ''' + @ColumnName + ''' AS ColumnName,
    ''' + @DataType + ''' AS DataType,

    ''' + 
    CASE 
        WHEN @DataType IN ('varchar','char','nvarchar','nchar')
            THEN @DataType + '(' + 
                 CASE WHEN @MaxLength = -1 THEN 'MAX'
                      ELSE CAST(@DeclaredLength AS VARCHAR(10)) END + ')'
        WHEN @DataType IN ('decimal','numeric')
            THEN @DataType + '(' + 
                 CAST(@Precision AS VARCHAR(10)) + ',' +
                 CAST(@Scale AS VARCHAR(10)) + ')'
        ELSE @DataType
    END + ''' AS TypeDefinition,

    ''' +
    CASE 
        WHEN @DataType = 'varchar'  THEN 'n bytes + 2 bytes'
        WHEN @DataType = 'nvarchar' THEN '2n bytes + 2 bytes'
        WHEN @DataType = 'char'     THEN CAST(@DeclaredLength AS VARCHAR) + ' bytes'
        WHEN @DataType = 'nchar'    THEN CAST(@DeclaredLength*2 AS VARCHAR) + ' bytes'
        WHEN @DataType = 'int'      THEN '4 bytes'
        WHEN @DataType = 'bigint'   THEN '8 bytes'
        WHEN @DataType = 'datetime' THEN '8 bytes'
        WHEN @DataType IN ('decimal','numeric')
            THEN '5–17 bytes depending on precision'
        ELSE CAST(@MaxLength AS VARCHAR) + ' bytes'
    END + ''' AS TheoreticalStorage,

    COUNT(*) AS TotalRows,
    SUM(DATALENGTH(' + QUOTENAME(@ColumnName) + ')) AS TotalBytesUsed,
    CAST(SUM(DATALENGTH(' + QUOTENAME(@ColumnName) + ')) / 1024.0 / 1024.0 AS DECIMAL(18,2)) AS TotalMBUsed,
    AVG(CAST(DATALENGTH(' + QUOTENAME(@ColumnName) + ') AS FLOAT)) AS AvgBytesPerRow

FROM ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ';
';

EXEC sp_executesql @SQL;
