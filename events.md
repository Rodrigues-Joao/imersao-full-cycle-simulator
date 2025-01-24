#Events

## Vamos receber

RouteCreated

- id
- distance
- directions
- - lat
- - lng

### Efeito colateral (calcular o frete e retornar o evento)

FreightCalculated

- route_id
- amount

---

DeliveryStarted

- route_id

### Efeito colateral

DriverModed

- route_id
- lat
  -lng
