//$('.add-table').click(function(){
//    table = $('select.from-table option:selected').text().trim()
//    $('#table-input').tagsinput('add', table)
//});

$('.add-join').click(function(){
    join_type = $("#join .dropdown-join").text().trim()
    join_table = $('#join select.join-tables option:selected').text().trim()
    con_key = 'on'
    fk_attr = $('#join input.join-fk-attributes').val()
    compare_key = '='
    join_attr = $('#join input.join-attributes').val()
    $('#join-input').tagsinput('add', join_type +' '+join_table+' '+con_key+' '+fk_attr+' '+compare_key+' '+join_attr)
});
$('.add-where').click(function(){
    logic_type = $('#where .dropdown-logic').text().trim()
    attr = $('#where select.attributes option:selected').text().trim()
    compare_type = $('#where .dropdown-compare').text().trim()
    value = $('#where input.value').val()
    if(compare_type=='between'){
        between_logis = 'and'
        between_value = $('#where input.between').val()
        value = value+' '+between_logis+' '+between_value
    } else if(compare_type=='in' || compare_type == 'not in') {
        items = $('#where input.in').tagsinput('items')
        value = JSON.stringify(items).replaceAll(',','，').replaceAll('"', '').replace('[','(').replace(']',')')
    }
    $('#where-input').tagsinput('add',logic_type+':'+attr+' '+compare_type+' '+value)
});
$('.add-group').click(function(){
    attr = $('#group select.attributes option:selected').text().trim()
    $('#group-input').tagsinput('add',attr)
});
$('.add-having').click(function(){
    logic_type = $('#having .dropdown-logic').text().trim()
    func_checked = $('#having .checkbox-having-attribute').prop('checked')
    attr = $('#having select.group-attributes option:selected').text().trim()
    if (func_checked) {
        func = $('#having select.functions option:selected').text().trim()
        attr = func+'('+attr+')'
    }
    compare_type = $('#having .dropdown-compare').text().trim()
    value = $('#having input.value').val()
    if(compare_type=='between'){
        between_logis = 'and'
        between_value = $('#having input.between').val()
        value = value+' '+between_logis+' '+between_value
    } else if(compare_type=='in' || compare_type == 'not in') {
        items = $('#having input.in').tagsinput('items')
        value = JSON.stringify(items).replaceAll(',','，').replace('[','(').replace(']',')')
    }
    $('#having-input').tagsinput('add',logic_type+':'+attr+' '+compare_type+' '+value)
});
$('.add-order').click(function(){
    func_checked = $('#order .checkbox-order').prop('checked')
    attr = $('#order select.group-attributes option:selected').text().trim()
    if (func_checked) {
        func = $('#order select.functions option:selected').text().trim()
        attr = func+'('+attr+')'
    }
    order_type = $('#order .dropdown-order').text().trim()
    $('#order-input').tagsinput('add',attr+' '+order_type)
});
$('.add-attribute').click(function(){
    func_checked = $('#attribute .checkbox-attribute').prop('checked')
    attr = $('#attribute select.group-attributes option:selected').text().trim()
    if (func_checked) {
        func = $('#attribute select.functions option:selected').text().trim()
        attr = func+'('+attr+')'
    }
    $('#attribute-input').tagsinput('add',attr)
})