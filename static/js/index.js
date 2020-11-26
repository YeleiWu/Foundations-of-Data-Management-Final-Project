$(function(){
    //init tabs UI
    $('#myTab a:first').tab('show')
    $("#myTab a").click(function(e){
        e.preventDefault();
        $(this).tab("show");
    });

    //init dbs
    current_db = initDbs()

    //init tables
    current_table = initTables()

    //init attributes and foreign key
    if (!isEmpty(current_table)) {
        initAttributes(current_db,[current_table])
        initFKs(current_db, current_table)
    }

    $('.clause .bootstrap-tagsinput').css('display', 'none')
    $('.having-clause .bootstrap-tagsinput').css('display', 'none')
    $(".alert").hide();
    $.cookie("group_attr", JSON.stringify([]))
    $.cookie("tables", JSON.stringify([current_table]))
    $(".offset").hide()
    $('#preview_card').hide()
    $('.translation').hide()
})

//dropdown change UI
$(".dropdown-menu .dropdown-item").click(function(){
    var selText = $(this).text();
    $('.dropdown-item').removeClass('active');
    $(this).addClass('active');
    $(this).parents('.dropdown').find('.dropdown-toggle').html(selText);
});
$(".compare .dropdown-item").click(function(){
    var cp_operation = $(this).text().trim();
    if (cp_operation == 'between') {
        $(this).parents().siblings('.bootstrap-tagsinput').hide()
        $(this).parents().siblings('.between').removeAttr('style');
        $(this).parents().siblings('.value').removeAttr('style');
    }else if(cp_operation == 'in' || cp_operation == 'not in' ){
        $(this).parents().siblings('.where-clause').hide()
        $(this).parents().siblings('.bootstrap-tagsinput').removeAttr('style');
        $(this).parents().siblings('.bootstrap-tagsinput').css('width', '30%')
    }else {
        $(this).parents().siblings('.bootstrap-tagsinput').hide()
        $(this).parents().siblings('.between').hide()
        $(this).parents().siblings('.value').removeAttr('style');
    }
});
// init dbs
function initDbs() {
    dbs = ajaxGetDbs()
    //display dbs
    dbs_select = $('select.database')
    dbs_select.empty()
    for(i in dbs){
        dbs_select.append('<option value='+dbs[i]+'>'+dbs[i]+'</option>')
    }
    current_db = $("select.database option:selected").text().trim()
    $.cookie('current_db', current_db)
    return current_db
}
// init tables
function initTables() {
    tables = ajaxGetTables($.cookie('current_db'))
    //display tables
    tables_select = $('select.table')
    tables_select.empty()
    for(i in tables){
        tables_select.append('<option value='+tables[i]+'>'+tables[i]+'</option>')
    }
    current_table = $("select.init-table option:selected").text().trim()
    $.cookie('current_table', current_table)
    // init from tables
    clear_input()
    return current_table
}

//init attributes
function initAttributes(db, tables){
    if (tables.length == 0) {
        return
    }
    attrs = ajaxGetAttributes(db, tables)
    console.log(attrs)
    cookie_attrs= []
    if (tables.length == 1) {
        cookie_attrs = attrs[tables[0]]

    } else if (tables.length > 1) {
        for(i in tables) {
            table_attrs = attrs[tables[i]]
            for (j in table_attrs) {
                cookie_attrs.push(tables[i]+'.'+table_attrs[j])
            }
        }

    }
//    cookie_attrs.splice(0, 0, "*")
    $.cookie("attr", JSON.stringify(cookie_attrs))
    //display attributes
    attributes_select = $('select.attributes')
    displayAttributes(attributes_select, cookie_attrs)
    attributes_select_group = $('select.group-attributes')
    displayAttributes(attributes_select_group, cookie_attrs)
}

//display attributes
function displayAttributes(node, attr_list) {
    node.empty();
    for(i in attr_list){
        node.append('<option value='+attr_list[i]+'>'+attr_list[i]+'</option>')
    }
}
function clear_input(){

    $('#join-input').tagsinput('removeAll')
}
//init from table
//function initFrom(){
//    clear_input()
////    $(".bootstrap-tagsinput span").remove();
////    $('input[data-role="tagsinput"]').tagsinput('removeAll')
//
//    current_table = $.cookie('current_table')
////    console.log('current-table:'+current_table)
//    $('#table-input').tagsinput('add', current_table)
//    if ($(".bootstrap-tagsinput span.badge").text() == current_table) {
//        $(".bootstrap-tagsinput span.badge span[data-role='remove']").hide()
//    }
//}


// init Fks
function initFKs(db, table){

    fks = ajaxGetFk(db)
    console.log(fks)
    if (JSON.stringify(fks) == '{}') {
        $(".join-part").hide()
        $('select.join-tables').empty()
        $('select.join-attributes').empty();
        $("input.join-fk-attributes").empty();
    } else {
        $(".join-part").show()
        attrs = ajaxGetAttributes(db, [table])
        //    console.log(attrs)
        $('select.join-tables').empty()
        fk_dict = {}
        for(var i in attrs[table]){
            key = table+'.'+attrs[table][i]
            if (Object.keys(fks).indexOf(key)!=-1){
                fk_dict[fks[key]] = key
                fk_table = fks[key].split(".")[0]
                $('select.join-tables').append('<option value='+fk_table+'>'+fk_table+'</option>')
                current_fk_table = $("select.join-tables option:selected").text().trim()
                if(fks[key].split('.')[0] == current_fk_table) {
                    $('input.join-attributes').val(key)
                    $("input.join-fk-attributes").val(fks[key])
                }
            }
        }
        $.cookie('fk',JSON.stringify(fk_dict))
        console.log($.cookie('fk'))
    }

}

// db change
function dbChange(obj){
    var db = obj.options[obj.selectedIndex].value;
    $.cookie('current_db', db)
    current_table = initTables()
    if (!isEmpty(current_table)) {
        //init attributes
        initAttributes(db, [current_table])
        initFKs(db, current_table)
    }
}

// table change
function tableChange(obj){
    var table = obj.options[obj.selectedIndex].value;
    $.cookie('current_table', table)
    $.cookie('tables', [table])
    initAttributes($.cookie('current_db'), [table])
//    $('input[data-role="tagsinput"]').tagsinput('removeAll')
    // init from tables
    clear_input()
    initFKs($.cookie('current_db'), table)
}
// fk table change
function fkTableChange(obj){
    var fk_table = obj.options[obj.selectedIndex].value;
    fks = JSON.parse($.cookie('fk'))
    fk_keys = Object.keys(fks)
    console.log(fk_keys)
    for (i in fk_keys) {
        if (fk_keys[i].split('.')[0] == fk_table) {
            $("input.join-fk-attributes").val(fk_keys[i])
            $('input.join-attributes').val(fks[fk_keys[i]])
        }
    }
}

//function updateTables(){
//    tables = $('#table-input').tagsinput('items')
//
//}

//$('#table-input').change(function(){
////    updateTables()
//    console.log($.cookie("tables"))
//})
$('#join-input').change(function(){
    tables = [$.cookie('current_table')]
    join = $('#join-input').tagsinput('items')
    for (i in join){
        t = join[i].split(' on ')[0].split(' ').pop()
        tables.push(t)
    }
    tables = Array.from(new Set(tables))
    $.cookie("tables", JSON.stringify(tables))
    initAttributes($.cookie('current_db'),tables)
    console.log($.cookie("tables"))
})

$('.checkbox-attribute').change(function(){
    isChecked = $(this).prop('checked')
    attributes_select = $('#attribute select.group-attributes')
    attr = JSON.parse($.cookie("attr"))
    group_attr = JSON.parse($.cookie("group_attr"))
    if (isChecked) {
        if (group_attr.length == 0) {
            attr = group_attr
        }
//        else {
//            attr.splice(0, 0, '*')
//        }
    } else {
        if (group_attr.length > 0) {
            attr = group_attr
        }
    }
    displayAttributes(attributes_select, attr)
});
$('.checkbox-having-attribute').change(function(){
    isChecked = $(this).prop('checked')
    attributes_select = $('#having select.group-attributes')
    attr = JSON.parse($.cookie("attr"))
    group_attr = JSON.parse($.cookie("group_attr"))
    if (isChecked) {
        if (group_attr.length == 0) {
            attr = group_attr
        }
//        else {
//            attr.splice(0, 0, '*')
//        }
    } else {
        if (group_attr.length > 0) {
            attr = group_attr

        }
    }
    displayAttributes(attributes_select, attr)
});
$('.checkbox-order').change(function(){
    isChecked = $(this).prop('checked')
    attributes_select = $('#order select.group-attributes')
    attr = JSON.parse($.cookie("attr"))
    group_attr = JSON.parse($.cookie("group_attr"))
    if (isChecked) {
        if (group_attr.length == 0) {
            attr = group_attr
        }
//        else {
//            attr.splice(0, 0, '*')
//        }
    } else {
        if (group_attr.length > 0) {
            attr = group_attr
        }
    }
    displayAttributes(attributes_select, attr)
});
$('#group-input').change(function(){
    cookie_group_attr = $('#group-input').tagsinput('items')
    $.cookie("group_attr", JSON.stringify(cookie_group_attr))
    //display attributes
    attributes_select_having = $('#having select.group-attributes')
    attributes_select_attribute = $('#attribute select.group-attributes')
    attributes_select_order = $('#order select.group-attributes')
    attr = JSON.parse($.cookie("attr"))
    if (cookie_group_attr.length > 0) {
//        attr.splice(0, 0, '*')
        if (!$('.checkbox-having-attribute').prop('checked')) {
            displayAttributes(attributes_select_having, cookie_group_attr)
        } else {

            displayAttributes(attributes_select_having, attr)
        }
        if (!$('.checkbox-attribute').prop('checked')){
            displayAttributes(attributes_select_attribute, cookie_group_attr)
        }else {
            displayAttributes(attributes_select_attribute, attr)
        }
        if (!$('.checkbox-order').prop('checked')){
            displayAttributes(attributes_select_order, cookie_group_attr)
        }else {
            displayAttributes(attributes_select_order, attr)
        }
    } else {
        if ($('.checkbox-having-attribute').prop('checked')) {
            displayAttributes(attributes_select_having, cookie_group_attr)
        } else {
            displayAttributes(attributes_select_having, attr)
        }
        if ($('.checkbox-attribute').prop('checked')){
            displayAttributes(attributes_select_attribute, cookie_group_attr)
        }else {
            displayAttributes(attributes_select_attribute, attr)
        }
        if ($('.checkbox-order').prop('checked')){
            displayAttributes(attributes_select_order, cookie_group_attr)
        }else {
            displayAttributes(attributes_select_order, attr)
        }
    }
})

$(".limit-input").change(function(){
    if ($(this).val().trim().length > 0){
        $(".offset").show()
    } else {
        $(".offset").hide()
    }
});


$('#preview_card').on('DOMSubtreeModified', '#sql_preview', function(){
    if ($('#sql_preview').text().trim().length == 0){
        $('#preview_card').hide()
    }else{
        $('#preview_card').show()
    }
});
$('.translation').on('DOMSubtreeModified', '.translation-type', function(){
    if ($('.translation-type').text().trim().length == 0){
        $('.translation').hide()
    }else{
        $('.translation').show()
    }
});

function preview(){
    sql_dict = produceSQLDict()
//    $("#previewModal .modal-body").text(ajaxGetSQL(sql_dict))
    $('#sql_preview').text(ajaxGetSQL(sql_dict))
}
function produceSQLDict(){
    var sql_dict = {};
    sql_dict['db'] = $.cookie('current_db')
    sql_dict['table'] = $.cookie('current_table')
    sql_dict['join'] = $('#join-input').tagsinput('items')
    sql_dict['where'] = $('#where-input').tagsinput('items')
    sql_dict['group'] = $('#group-input').tagsinput('items')
    sql_dict['having'] = $('#having-input').tagsinput('items')
    sql_dict['order'] = $('#order-input').tagsinput('items')
    sql_dict['attribute'] = $('#attribute-input').tagsinput('items')
    sql_dict['limit'] = $('.limit-input').val()
    sql_dict['offset'] = $('.offset-input').val()
    return sql_dict
}
function get_display_columns(sql_dict){
    attr = []
    if (sql_dict['attribute'].length == 0) {
        if (sql_dict['group'].length == 0){
            attr = JSON.parse($.cookie("attr"))
        }else{
            attr = sql_dict['group']
        }


    } else {
        attr = sql_dict['attribute']
    }
    columns = []
    for(i in attr){
        if (attr[i] == 'comments' || attr[i] == 'review.comments'){
            columns.push({'title':attr[i],
            'render':function(data, type, row, meta){
                if (type === 'display') {
                    if (data.length > 40) {
                        return '<span title="' + data + '">' + data.substr(0, 40) + '...</span>';
                    } else {
                        return '<span title="' + data + '>' + data + '</span>';
                    }
                }
                return data;
            }})
        } else {
            columns.push({'title':attr[i]})
        }

    }
    return columns
}
function query(){
    queryResult = null
    db = $("select.database option:selected").text()
    sql_dict = produceSQLDict()
    $('#sql_preview').text(ajaxGetSQL(sql_dict))
    columns = get_display_columns(sql_dict)
    sql = ajaxGetSQL(sql_dict)
//    console.log('sql:'+sql)
    events_table_template = {
        "lengthMenu": [5, 10, 15, 20],
//        "processing": true,
        "serverSide": false,
        "ordering":false,
        "bDestroy": true,
        "ajax": {
            "url": "/query",
            "type": "POST",
            "dataType": "json",
            "dataSrc":"",
            "data": {'db': db, 'sql': sql},
        },
        "columns":columns
    };
    var table_modal = $('#example').DataTable(events_table_template);
    console.log("events_table_template:",events_table_template)
//    table_modal.clear().draw()
//    $("#queryModal .modal-body").text(queryResult)
}

$('#queryModal').on('hidden.bs.modal', function () {
    $("#queryModal .modal-body").empty()
    $("#queryModal .modal-body").append('<table id="example" class="display" style="width:100%"><thead></thead></table>')
//$('#example').dataTable();
});

function translate_display(translation_type,sql_dict){
//    if (translation_type == 'MapView') {
//            console.log("mapview")
//            $('#mapModal').modal('show')
//    }
            $('.btn-translate').empty()
        $('.btn-translate').append('<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>Translating...')

    result = ajaxTranslate(translation_type, sql_dict)
    if (translation_type == 'MapView'){
                $("#queryTL").text('Preview')
//                $("#queryTL").attr('data-target','#mapModal')
            } else {
                $("#queryTL").text('Query')
            }

}

$('.btn-translate').click(function(){
//    $(this).append('<span class="spinner-border spinner-border-sm" id="spinner" role="status" aria-hidden="true"></span>')
//    $('#waitModal .modal-title').text('Translation')
//    $('#waitModal .modal-body').text('Translating...')
//    $("#waitModal").modal({
//      backdrop: "static", //remove ability to close modal with click
//      keyboard: false, //remove option to close with keyboard
//      show: true //Display loader!
//    });

    sql_dict = produceSQLDict()
    translation_type = $("select.languages option:selected").text()
    translate_display(translation_type,sql_dict)
})
function queryTL(){
    type = $('.translation-type').text()
    sql_dict = produceSQLDict()

    translate_display(type,sql_dict)
//    $('.translation-content').text(result)

    $('#queryTLModal').find(".modal-title").text(type + ' Result')
    $('#queryTLModal').find(".modal-body").empty()
    if(type == 'MapView'){
        ajaxMapQueryTL(type, sql_dict)

    } else {
        $('#queryTL').empty()
        $('#queryTL').append('<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>Query...')
        ajaxQueryTL(type, sql_dict)
    }
    if (translation_type == 'MapView'){
                $("#queryTL").text('Preview')
//                $("#queryTL").attr('data-target','#mapModal')
            } else {
                $("#queryTL").text('Query')
            }

//     else {
//    ajaxQueryTL(type, sql_dict)
//    if(type == 'MongoDB'){
//        result = JSON.parse()
//        $('#queryTLModal').find(".modal-body").append('<ol>')
//        for (i in result){
//            $('#queryTLModal').find(".modal-body ol").append('<li>'+JSON.stringify(result[i]) + '</li>')
//        }
//        $('#queryTLModal').find(".modal-body").append('</ol>')
//    } else if (type == 'Dataframe' || type == 'Spark' ) {
//
//        result = ajaxQueryTL(type, sql_dict)
//        $('#queryTLModal').find(".modal-body").append(result)
//    }
//    $('#queryTLModal').modal('show')
//    }
}
//function queryMap() {
//    result = ajaxQueryTL(type, sql_dict)
//}


$('#reset').on('click',function(){
//    alert("reset")
    $('#join-input').tagsinput('removeAll')
    $('#where-input').tagsinput('removeAll')
    $('#group-input').tagsinput('removeAll')
    $('#having-input').tagsinput('removeAll')
    $('#order-input').tagsinput('removeAll')
    $('#attribute-input').tagsinput('removeAll')
    $('.limit-input').val('')
    $('.offset-input').val('')
})
function isEmpty(obj){
    if(typeof obj == "undefined" || obj == null || obj == ""){
        return true;
    }else{
        return false;
    }
}