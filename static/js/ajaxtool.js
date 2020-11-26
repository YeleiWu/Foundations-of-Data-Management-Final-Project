// get dbs
function ajaxGetDbs(){
    dbs = null
    $.ajax({
        async: false,
        type: 'GET',
        url: '/getDbs',
        success:function(result){
            dbs = result.dbs
        },
         error: function(error){
            console.log("error")
         }
    });
    return dbs
}

//get tables
function ajaxGetTables(db){
    tables = null
    $.ajax({
        async: false,
        type: 'POST',
        url: '/getTables',
        dataType:"json",
        data: {'db': db},
        success:function(result){
            tables = result.tables
        },
         error: function(error){
            console.log("error")
         }
    });
    return tables
}

//get attributes for tables(array)
function ajaxGetAttributes(db, tables){
    attrs = null
    console.log(tables)
    $.ajax({
        async: false,
        type: 'POST',
        url: '/getAttrs',
        dataType:"json",
        data: {'db':db, 'tables': tables},
        success:function(result){
            attrs = result.attributes
        },
         error: function(error){
            console.log("error")
         }
    });
    return attrs
}

//get fks
function ajaxGetFk(db){
    fks = null
    $.ajax({
        async: false,
        type: 'POST',
        url: '/getFks',
        dataType:"json",
        data: {'db': db},
        success:function(result){
            fks = result.fks
        },
         error: function(error){
            console.log("error")
         }
    });
    return fks
}

// get sql
function ajaxGetSQL(sql_dict){
    sql = ''
    $.ajax({
        async: false,
        type: 'POST',
        url: '/getSql',
        dataType:"json",
        data: {'sql_dict': JSON.stringify(sql_dict)},
        success:function(result){
            sql = result.sql
        },
         error: function(error){
            console.log("error")
         }
    });
    return sql
}

// translation
function ajaxTranslate(translation_type, sql_dict){
    tl = ''
    $.ajax({
        async: true,
        type: 'POST',
        url: '/translate',
        dataType:"json",
        data: {'type':translation_type, 'sql_dict': JSON.stringify(sql_dict)},
        beforeSend: function(){       //ajax发送请求时的操作，得到请求结果前有效\

//                        $('#waitModal').modal({
//                            backdrop:'static'      //<span style="color:#FF6666;">设置模态框之外点击无效</span>
//                        });
//                        $('#waitModal .modal-title').text('Translating...')
//                        $('#waitModal').modal('show');   //弹出模态框
                    },
        complete: function(){
        $('.btn-translate').empty()
        $('.btn-translate').append('Translate')
        },
        success:function(result){
//            $(".alert").show();

            result = result.tl
            if (result != ''){
            $('.translation-type').text(translation_type)
            if (translation_type == 'MongoDB'){
                console.log("j")
                $('.translation-content').text(result)
            }
            else if (translation_type == 'MapView') {
                $('.translation-content').empty()
                $('.translation-content').append('<p>QueryParameters = new Query({</p>')
    //            keys = Object.keys(result)
                for (key in result){
                    value = result[key]
                    if(value == '*'){
                        value = "['*']"
                    }else if (value == ''){
                        value = "''"
                    }
                    if (key == 'outFields') {
                    value = '['+value +']'
                    }
                    $('.translation-content').append('<p>'+key+': '+ value+',</p>')
                }
                $('.translation-content').append('<p>})</p>')

            }
            else {
                $('.translation-content').empty()
                for (i in result) {
                    console.log('i',result[i])
                    $('.translation-content').append('<p>'+result[i]+'</p>')
                }
            }

        }
        },
         error: function(error){
//            $('#waitModal .modal-body').text('Failed!')
            console.log(error)
         }
    });
    return tl
}
function ajaxMapQueryTL(type, sql_dict){
    result = null
    ajx = $.ajax({
        async: false,
        type: 'POST',
        url: '/queryTL',
        dataType:"json",
        data: {'type':type, 'sql_dict': JSON.stringify(sql_dict)},
        success:function(r){


            result = r.tl_result


            $('#queryTL').empty()
            $('#queryTL').append('Query')
            query = 'QueryParameters = new Query({'
        //        keys = Object.keys()
                    for (key in result){
                        value = result[key]
                        if(value == '*'){
                            value = "['*']"
                        } else if (value == ''){
                            value = "''"
                        }
                        query = query + key+': '+ value+','
                    }
                query = query + '})'

                $('.map-query code').text(query)
                $('#mapModal').modal('show')
//                return result
        },
         error: function(error){
//            $('#waitModal .modal-body').text(error)
            console.log(error)
         }
    })
    console.log('successresult',ajx)
    return result

}
function ajaxQueryTL(type, sql_dict){
//    $('#waitModal').modal('show')
//    $('#waitModal .modal-title').text('Translation')
//    $('#waitModal .modal-body').text('Translating...')
    result = null
    ajx = $.ajax({
        async: true,
        type: 'POST',
        url: '/queryTL',
        dataType:"json",
        data: {'type':type, 'sql_dict': JSON.stringify(sql_dict)},
        beforeSend: function(){       //ajax发送请求时的操作，得到请求结果前有效\

      },
        complete: function(){
//        console.log('result',result)
//            return result
        },
        success:function(result){
//            $('#waitModal .modal-body').text('success')
//            $('#waitModal').modal('hide')

            console.log('successresult',result)
            result = result.tl_result

            $('#queryTL').empty()
            $('#queryTL').append('Query')
            if (type == 'MapView') {
                query = 'QueryParameters = new Query({'
        //        keys = Object.keys()
                    for (key in result){
                        value = result[key]
                        if(value == '*'){
                            value = "['*']"
                        } else if (value == ''){
                            value = "''"
                        }
                        query = query + key+': '+ value+','
                    }
                query = query + '})'

                $('.map-query code').text(query)
                $('#mapModal').modal('show')

            }else {
                if(type == 'MongoDB'){
                    result = JSON.parse(result)
                    $('#queryTLModal').find(".modal-body").append('<ol>')
                    for (i in result){
                        $('#queryTLModal').find(".modal-body ol").append('<li>'+JSON.stringify(result[i]) + '</li>')
                    }
                    $('#queryTLModal').find(".modal-body").append('</ol>')
                } else if (type == 'Dataframe' || type == 'Spark' ) {
                    $('#queryTLModal').find(".modal-body").append(result)
                }

                $('#queryTLModal').modal('show')
            }

        },
         error: function(error){
//            $('#waitModal .modal-body').text(error)
            console.log(error)
         }
    })
    ajx.done(function(r){
    console.log("jsresult:",typeof(r))
    console.log("jsresult:",r)
    result = r.tl_result
    })
//    console.log("jsresult:",typeof(result))
//    console.log("responseText",result.responseText)
//    console.log("responseJSON",result.responseJSON)
//    return result;
    return result
}